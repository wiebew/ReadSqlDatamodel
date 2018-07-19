Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Import-Module -Name SqlServer


# declared as global so we can inspect the data later after running functions on the commandline
#$global:rdd = $null
#$global:sqs = $null

# this function copies the properties of the Row to a PSCustomObject and returns the PSCustomObject
# thus returning a clean object with name,value pairs for each field in the datarow
Function FillObject {
	Param ($Row)

	$object = [PSCustomObject]@{ }

	$props = $Row | Get-Member -MemberType Property
	foreach ( $prop in $props ) {
		$name = $prop.Name
		$value = $Row.($prop.Name)
		$object | Add-Member -MemberType NoteProperty -Name $name -Value $value
	}
	Write-Output $object
}

Function FillObjectDashNames {
	# same sas FillObject, only it also
	# converts all underscores in the property name to dashes, rdd convention  ¯\_(ツ)_/¯
	Param ($Row)

	$object = [PSCustomObject]@{ }

	$props = $Row | Get-Member -MemberType Property
	foreach ( $prop in $props ) {
		$value = $Row.($prop.Name)
		$name = $prop.Name.Replace("_","-")

		$object | Add-Member -MemberType NoteProperty -Name $name -Value $value
	}
	Write-Output $object
}

# returns a date string in ISO Date format, this format will be recognized by SqlServer independent of locale settings on dateformat
# Example: 2012-06-18T10:34:09
Function GetIsoDateString {
	Param ( [datetime]$Date )

	Write-Output $Date.ToString("yyyy-MM-ddTHH:mm:ss")
}

Function RddCreateTablesHash {
	Param ( $TableRows,  $ColumnRows )
	$Tables = @{}
	foreach ($TableRow in $TableRows) {
		$Table = FillObject -Row $TableRow
		$Table | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Table | Add-Member -MemberType NoteProperty -Name "__columnaliases" -Value @{}
		$Table | Add-Member -MemberType NoteProperty -Name "__indexes" -Value @{}
		$Table | Add-Member -MemberType NoteProperty -Name "__indexaliases" -Value @{}
		$Tables[$Table.DB_REC_MNEM] = $Table
	}

	foreach ($ColumnRow in $ColumnRows) {
		$Column = FillObject -Row $ColumnRow
		$Table = $Tables[$Column.DB_REC_MNEM]
		$Table.__columns[$Column.ITEM_INT_MNEM] = $Column
		if ( ![string]::IsNullOrEmpty($Column.ITEM_INT_ALIAS ) ) {
			$Table.__columnaliases[$Column.ITEM_INT_ALIAS] = $Column
		}
	}

	Write-Output $tables
}

Function RddCreateTableAliasesHash {
	Param ($tables)

	$Aliases = @{}
	foreach ( $key in $tables.Keys ) {
		$table = $tables[$key]
		if ( ![string]::IsNullOrEmpty($table.DB_REC_ALIAS) ) {
			$Aliases[$table.DB_REC_ALIAS] = $table
		}
	}
	Write-Output $Aliases
}

Function RddCreateIndexAliasesHash {
	Param ($indexes)

	$Aliases = @{}
	foreach ( $key in $indexes.Keys ) {
		$index = $indexes[$key]
		if ( ![string]::IsNullOrEmpty($index.DB_SLEUT_ALIAS) ) {
			$Aliases[$index.DB_SLEUT_ALIAS] = $index
		}
	}
	Write-Output $Aliases
}


Function RddCreateIndexeshash {
	Param (  $IndexRows, $ColumnRows, $Tables )

	$Indexes = @{}
	foreach ($Row in $IndexRows) {
		$Index = FillObject -Row $Row
		$Index | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$key = $Index.DB_REC_MNEM + "-" + $Index.SOORT_DB_SL + "-" + $Index.DB_SL_VOLG_NR
		$Indexes[$key] = $Index

		$Tables[$Index.DB_REC_MNEM].__indexes[$Index.DB_SLEUT_NAAM] = $Index
	}

	foreach ( $Row in $ColumnRows) {
		$Column = FillObject -Row $Row
		$key = $Column.DB_REC_MNEM + "-" + $Column.SOORT_DB_SL + "-" + $Column.DB_SL_VOLG_NR
		$Indexes[$key].__columns[$Column.ITEM_INT_MNEM] = $Column
	}
	Write-Output $Indexes
}

Function RddCreateDomeinenHash {
	Param ( $DomeinWaardeRows )

	$Domeinen = @{}
	foreach ($Row in $DomeinWaardeRows) {
		$DomeinWaarde = FillObject -Row $Row
		if (!$Domeinen.Contains($DomeinWaarde.DOMEIN_MNEM)) {
			$Domeinen[$DomeinWaarde.DOMEIN_MNEM] = New-Object System.Collections.Generic.List[System.Object]
		}
		$Domeinen[$DomeinWaarde.DOMEIN_MNEM].Add($DomeinWaarde)
	}
	Write-Output $Domeinen
}

Function RddCreateAttributenHash {
	Param ( $AttribuutRows )

	$Attributen = @{}
	foreach ($AttribuutRow in $AttribuutRows ) {
		$Attribuut = FillObject -Row $AttribuutRow
		$Attributen[$Attribuut.ATTRIBUUT_MNEM] = $Attribuut
	}

	Write-Output $Attributen
}

Function RddCreateBeperkingenHash {
	Param ( $BeperkingRows )

	$Beperkingen = @{}
	foreach ($BeperkingRow in $BeperkingRows ) {
		$Beperking = FillObject -Row $BeperkingRow
		$Beperkingen[$Beperking.ITEM_INT_MNEM] = $Beperking
	}

	Write-Output $Beperkingen
}

Function RddCreateToevoegingenList {
	Param( $ToevoegingRows )

	$Toevoegingen = New-Object System.Collections.Generic.List[System.Object]
	foreach ($ToevoegingRow in $ToevoegingRows ) {
		$Toevoeging = FillObject -Row $ToevoegingRow
		$Toevoegingen.Add($Toevoeging)
	}

	Write-Output $Toevoegingen
}

Function RddCollectMetaData {
	Param(
		$Schema, $SqlServerInstance, $Database, [datetime] $DateStamp
	)

	$IsoDate = GetIsoDateString $DateStamp
	Write-Host "** Querying RDD Records for rdd schema $Schema on date $IsoDate"
	# ophalen beperkingen, deze zijn vastgelegd in DB_OPB_BEPERK. Indien het veld BEPERK_TEKST leeg is, dan zit de beperking in de tabel DOMEIN_WAARDE
	# deze is op te halen via DB_OPB_beperk.ITEM_INT_MNEM -> ATTRIBUUT.ATTRIBUUT_MNEM en dan ATTRIBUUT.DOMEIN_ATT -> DOMEIN_WAARDE.DOMEIN_MNEM
	# DOMEIN_WAARDE bevat dan 1 of meer toegestane waarden.
	# we gebruiken joins op attribuut en domein_waarde queries omdat deze tabellen erg groot zijn en een relatief klein aantal records gebruikt wordt (bijv de RDD definitie gebruikt slechts 17 vd ca 21.000 records)
	$BeperkingRows = Invoke-Sqlcmd "select * from RDD.DB_OPB_BEPERK where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, ITEM_INT_MNEM, BEPERK_IDENT" -ServerInstance $SqlServerInstance -Database $Database
	$AttribuutSubsetRows =  Invoke-Sqlcmd "select DISTINCT A.* from RDD.ATTRIBUUT as A join RDD.DB_OPB_beperk AS B ON (A.ATTRIBUUT_MNEM = B.ITEM_INT_MNEM) where B.DB_SCHEMA_MNEM = `'$Schema`' order by ATTRIBUUT_MNEM" -ServerInstance $SqlServerInstance -Database $Database
	$DomeinWaardeSubsetRows =  Invoke-Sqlcmd "select distinct D.* from RDD.DOMEIN_WAARDE as D join RDD.ATTRIBUUT as A ON (D.DOMEIN_MNEM = A.DOMEIN_ATT ) join RDD.DB_OPB_beperk AS B ON (A.ATTRIBUUT_MNEM = B.ITEM_INT_MNEM) where B.DB_SCHEMA_MNEM = `'$Schema`' order by D.DOMEIN_MNEM, D.DOM_WAARDE_VNR" -ServerInstance $SqlServerInstance -Database $Database

	# ophalen tabellen en kolommmen
	$TableRows = Invoke-Sqlcmd "select * from RDD.DB_RECORD where DB_SCHEMA_MNEM = `'$Schema`' AND (B_DAT_DBR <= '$IsoDate') AND ('$IsoDate' <= E_DAT_DBR) order by DB_SCHEMA_MNEM, DB_REC_MNEM" -ServerInstance $SqlServerInstance -Database $Database
	$TableColumnRows = Invoke-Sqlcmd "select * from RDD.DB_REC_OPB where DB_SCHEMA_MNEM = `'$Schema`' AND (B_DAT_DRO <= '$IsoDate') AND ('$IsoDate' <= E_DAT_DRO) order by DB_SCHEMA_MNEM, DB_REC_MNEM, ITEM_INT_MNEM" -ServerInstance $SqlServerInstance -Database $Database

	# ophalen indexen en kolommmen
	$IndexRows =  Invoke-Sqlcmd "select * from RDD.DB_SLEUTEL where DB_SCHEMA_MNEM = `'$Schema`' AND (B_DAT_DSL <= '$IsoDate') AND ('$IsoDate' <= E_DAT_DSL) order by DB_SCHEMA_MNEM, DB_REC_MNEM, SOORT_DB_SL, DB_SL_VOLG_NR" -ServerInstance $SqlServerInstance -Database $Database
	$IndexColumnRows =  Invoke-Sqlcmd "select * from RDD.DB_SLEUT_OPB where DB_SCHEMA_MNEM = `'$Schema`' AND (B_DAT_DSO <= '$IsoDate') AND ('$IsoDate' <= E_DAT_DSO) order by DB_SCHEMA_MNEM, DB_REC_MNEM, SOORT_DB_SL, DB_SL_VOLG_NR" -ServerInstance $SqlServerInstance -Database $Database

	# ophalen toevoeginen
	# eigenlijk custom scripts, hiervan is lastig te valideren of ze zijn uitegvoerd op de target database.
	$ToevoegingRows =  Invoke-Sqlcmd "select * from RDD.DB_SCHEMA_TOEV where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, SCH_TOEV_TEKST" -ServerInstance $SqlServerInstance -Database $Database

	Write-Host "Building RDD Beperkingen structure"
	$beperkingen = RddCreateBeperkingenHash -BeperkingRows $BeperkingRows
	$attributenUsed = RddCreateAttributenHash -AttribuutRows $AttribuutSubsetRows
	$domeinenUsed = RddCreateDomeinenHash -DomeinWaardeRows $DomeinWaardeSubsetRows
	Write-Host "Building RDD Tables structure"
	$tables = RddCreateTablesHash -TableRows $TableRows -ColumnRows $TableColumnRows
	$table_aliases = RddCreateTableAliasesHash -tables $tables
	Write-Host "Building RDD Indexes structure"
	$indexes = RddCreateIndexesHash -IndexRows $IndexRows -ColumnRows $IndexColumnRows -Tables $tables
	$index_aliases = RddCreateIndexAliasesHash -indexes $indexes
	Write-Host "Building RDD Toevoegingen structure"
	$toevoegingen = RddCreateToevoegingenList -ToevoegingRows $ToevoegingRows

	Write-Host "Found $($beperkingen.count) beperkingen, $($TableRows.count) tables, $($IndexRows.count) indexes, 0 toevoegingen"

	Write-Output @{ Tables = $tables; TableAliases = $table_aliases; Indexes = $indexes; IndexAliases = $index_aliases; Toevoegingen = $toevoegingen; Beperkingen = $beperkingen; Attributen = $attributenUsed; Domeimen = $domeinenUsed }
}

Function SqsCreateTablesHash {
	Param ( $TableRows,  $ColumnRows )
	$Tables = @{}

	$table_lookup = @{}

	foreach ($TableRow in $TableRows) {
		$Table = FillObjectDashNames -Row $TableRow
		$Table | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Table | Add-Member -MemberType NoteProperty -Name "__indexes" -Value @{}
		$Table | Add-Member -MemberType NoteProperty -Name "__foreign_keys" -Value @{}
		$Table.name = $Table.name.replace("_","-")
		$table_lookup[$Table.'object-id'] = $Table
		$Tables[$Table.name] = $Table
	}

	foreach ($ColumnRow in $ColumnRows) {
		$Column = FillObjectDashNames -Row $ColumnRow
		$table_lookup[$Column.'object-id'].__columns[$column.name.replace("_","-")] = $Column
	}

	Write-Output $tables
}

Function SqsCreateIndexesHash {
	Param (  $IndexRows, $ColumnRows, $Tables )

	$Indexes = @{}
	foreach ($Row in $IndexRows) {
		$Index = FillObjectDashNames -Row $Row
		$Index | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Index.'TABLE-NAME' = $Index.'TABLE-NAME'.replace("_","-")
		$Index.'name' = $Index.'name'.replace("_","-")
		$key = "$($Index.'TABLE-NAME')@$($Index.'name')"
		$Indexes[$key] = $Index

		$Tables[$Index.'TABLE-NAME'].__indexes[$Index.'name'] = $Index
	}

	foreach ( $Row in $ColumnRows) {
		$Column = FillObjectDashNames -Row $Row
		$Column.'TABLE-NAME' = $Column.'TABLE-NAME'.replace("_","-")
		$Column.'INDEX-NAME' = $Column.'INDEX-NAME'.replace("_","-")
		$Column.'COLUMN-NAME' = $Column.'COLUMN-NAME'.replace("_","-")
		$key = "$($Column.'TABLE-NAME')@$($Column.'INDEX-NAME')"
		$Indexes[$key].__columns[$Column.'COLUMN-NAME'] = $Column
	}
	Write-Output $Indexes
}

Function SqsCreateForeignKeysHash {
	Param ($FkRows, $FkColumnRows )

	$Fks = @{}
	foreach ($Row in $FkRows) {
		$Index = FillObjectDashNames -Row $Row
		$Index | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Index.'TABLE-NAME' = $Index.'TABLE-NAME'.replace("_","-")
		$Index.'FOREIGN-KEY-NAME' = $Index.'FOREIGN-KEY-NAME'.replace("_","-")
		$key = "$($Index.'TABLE-NAME')@$($Index.'FOREIGN-KEY-NAME')"
		$Fks[$key] = $Index

		$Tables[$Index.'TABLE-NAME'].__foreign_keys[$Index.'FOREIGN-KEY-NAME'] = $Index
	}

	foreach ( $Row in $FkColumnRows) {
		$Column = FillObjectDashNames -Row $Row
		$Column.'TABLE-NAME' = $Column.'TABLE-NAME'.replace("_","-")
		$Column.'FOREIGN-KEY-NAME' = $Column.'FOREIGN-KEY-NAME'.replace("_","-")
		$Column.'CONSTRAINT-COLUMN-NAME' = $Column.'CONSTRAINT-COLUMN-NAME'.replace("_","-")
		$Column.'REFERENCED-TABLE' = $Column.'REFERENCED-TABLE'.replace("_","-")
		$Column.'REFERENCED-COLUMN-NAME' = $Column.'REFERENCED-COLUMN-NAME'.replace("_","-")
		$key = "$($Column.'TABLE-NAME')@$($Column.'FOREIGN-KEY-NAME')"
		$Fks[$key].__columns[$Column.'CONSTRAINT-COLUMN-NAME'] = $Column
	}
	Write-Output $Fks
}

Function SqsCollectMetaData {
	Param(
		$Schema, $SqlServerInstance, $Database
	)
	Write-Host "** Querying Physical Sql Server for SQL schema $Schema"
	$TableRows = Invoke-Sqlcmd "select * from sys.tables where schema_name(schema_id) = '$SqlSchema'" -ServerInstance $SqlServerInstance -Database $SqlDatabase
	$ColumnRows = Invoke-Sqlcmd "SELECT t.name as TABLE_NAME, ty.name as COLUMN_TYPE, c.* FROM sys.columns c JOIN sys.tables t ON (t.object_id = c.object_id) JOIN sys.types ty on ty.system_type_id = c.system_type_id where schema_name(t.schema_id) = '$SqlSchema'" -ServerInstance $SqlServerInstance -Database $SqlDatabase
	$IndexRows = Invoke-Sqlcmd "select i.*, t.name as TABLE_NAME from sys.indexes i inner join sys.tables t on i.object_id = t.object_id where schema_name(t.schema_id) = '$SqlSchema'" -ServerInstance $SqlServerInstance -Database $SqlDatabase
	$IndexColumnRows =  Invoke-Sqlcmd "select t.name as TABLE_NAME, i.name as INDEX_NAME, tc.name as COLUMN_NAME, ic.* from sys.index_columns ic inner join sys.indexes i ON (i.object_id = ic.object_id and i.index_id = ic.index_id) inner join sys.tables t on (i.object_id = t.object_id) inner join sys.columns tc on ic.column_id = tc.column_id and ic.object_id = tc.object_id where schema_name(t.schema_id) = '$SqlSchema' order by t.name, i.name, index_column_id" -ServerInstance $SqlServerInstance -Database $Database
	$FkRows = Invoke-Sqlcmd  "select t.name as TABLE_NAME, fk.name as FOREIGN_KEY_NAME, fk.* from sys.foreign_keys fk join sys.tables t on (fk.parent_object_id = t.object_id) where schema_name(t.schema_id) = '$SqlSchema'"  -ServerInstance $SqlServerInstance -Database $Database
	$FkColumnRows = Invoke-Sqlcmd  "SELECT t.name AS TABLE_NAME, f.name AS FOREIGN_KEY_NAME, COL_NAME(fc.parent_object_id, fc.parent_column_id) AS CONSTRAINT_COLUMN_NAME,
									OBJECT_NAME (f.referenced_object_id) AS REFERENCED_TABLE, COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS REFERENCED_COLUMN_NAME, fc.*
									FROM sys.foreign_keys AS f
									INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
									INNER JOIN sys.tables as t on f.parent_object_id = t.object_id
									WHERE schema_name(t.schema_id) = '$SqlSchema' ORDER BY t.name, f.name, fc.constraint_column_id"  -ServerInstance $SqlServerInstance -Database $Database

	Write-Host "Building SQS Tables structure"
	$Tables = SqsCreateTablesHash -TableRows $TableRows -ColumnRows $ColumnRows
	Write-Host "Building SQS Indexes structure"
	$indexes = SqsCreateIndexesHash -IndexRows $IndexRows -ColumnRows $IndexColumnRows -Tables $tables

	$ForeignKeys = SqsCreateForeignKeysHash -FkRows $FkRows -FkColumnRows $FkColumnRows

	Write-Host "Found $($TableRows.count) tables, $($IndexRows.count) indexes"
	Write-Output  @{ Tables = $Tables; Indexes = $Indexes; ForeignKeys = $ForeignKeys };
}

# returns a typestring (RDD formatted) for a column
# thats has been collected from a physical SQS database
Function GetRddTypeString {
	Param( $sql_column )

	$base = ($sql_column.'COLUMN-TYPE').ToUpper()

	# SYSNAME is alternative name for NVARCHAR (also implies NULL is not allowed)
	# rename to NVARCHAR for RDD compatibility
	if ( $base.Equals('SYSNAME') ) {
			$base = "NVARCHAR"
	}
	$size = $($sql_column.'max-length')

	if ( $base.Equals('INT') -and $sql_column.'is-identity') {
		$base = "INT IDENTITY"
	}
	
	# nvarchar size is 2x too big, NVARCHAR is always stored in two bytes,
	# except when NVARCHAR is -1. Divide it by 2 to get original declaration size
	if ( $base.Equals('NVARCHAR') -and ($size -ne -1)  ) {
		$size = $size / 2
	}

	if ( ('VARCHAR', 'NVARCHAR', 'CHARACTER', 'VARCHAR2', 'VARBINARY').Contains( $base ) ) {
		# append size to type, if size = -1, change name to MAX for RDD compatibility
		if ($size -eq -1 ) {
			$size = "MAX"
		}
		Write-Output "$base($size)".ToUpper()
	} else {
		if ( ('NUMERIC', 'NUMBER').Contains( $base ) ) {
			$precision = $sql_column.precision
			$scale = $sql_column.scale
			if ( $scale -eq 0 ) {
				Write-Output "$base($precision)".ToUpper()
			} else {
				Write-Output "$base($precision,$scale)".ToUpper()
			}
		} else {
			Write-Output $base
		}
	}
}

Function SqsCompareColumns {
	Param( $rdd_column, $sql_column)

	# compare type of the column
	$sql_type = GetRddTypeString -sql_column $sql_column
	if ( !($rdd_column.'ITEM_INT_IMPL'.Equals($sql_type) ) ) {
		Write-Host "$($sql_column.'TABLE-NAME').$($sql_column.name) Different type $($rdd_column.'ITEM_INT_IMPL') expected, found $sql_type in the database"
	}
	# compare non-nullable
	if (($rdd_column.'DATA_ITEM_AANW' -eq "J") -ne !($sql_column.'is-nullable')) {
		Write-Host "$($sql_column.'TABLE-NAME').$($sql_column.name) Different non nullable setting: DATA_ITEM_AANW=$($rdd_column.'DATA_ITEM_AANW') expected, found is_nullable = $(($sql_column.'is-nullable')) in the database"
	}
}

# searches for an item in the hash on the $key, if not found
# and $aliaskey is not empty then it searches for the item using the alias
# return null if not gounf
Function FindItemWithAlias {
	Param ($hash, $key, $aliaskey)

	if ( $hash.Contains($key) ) {
		Write-Output $hash[$key]
	} else {
		if ( ![string]::IsNullOrEmpty($aliaskey) -and $hash.Contains($aliaskey) ) {
			Write-Output $hash[$aliaskey]
		} else {
			Write-Output $null
		}
	}
}

# searches in the two hashes for an item
# this is used if we search from the physical database back to rdd
# then we need to look in two indexes (hash on normal name and hash on
# alias name)
Function FindItemInTwoHashes {
	Param ($hash1, $hash2, $key)

	if ( $hash1.Contains($key) ) {
		Write-Output $hash1[$key]
	} else {
		if ( $hash2.Contains($key) ) {
			Write-Output $hash2[$key]
		} else {
			Write-Output $null
		}
	}
}

Function SqsCompareTables {
	Param(
		$rdd, $sqs
	)

	foreach ($rddkey in $rdd.Tables.keys) {
		$rddtable = $rdd.Tables[$rddkey]
		$sqstable = FindItemWithAlias -hash $sqs.Tables -key $rddkey -aliaskey $rddtable.DB_REC_ALIAS
		if ( ![string]::IsNullOrEmpty($sqstable) ) {
			$rdd_columns = $rddtable.__columns
			$sqs_columns = $sqstable.__columns

			foreach ( $rddcolkey in $rdd_columns.keys ) {
				$rdd_column = $rdd_columns[$rddcolkey]
				$sql_column = FindItemWithAlias -hash $sqs_columns -key $rddcolkey -aliaskey $rdd_column.ITEM_INT_ALIAS

				if ( ![string]::IsNullOrEmpty($sql_column) ) {
					SqsCompareColumns -rdd_column $rdd_column -sql_column $sql_column
				} else {
					Write-Host ("Column $($rdd_column.'ITEM_INT_MNEM') of Table $($rddtable.'DB_REC_MNEM') is in the RDD, but was not found in the physical database")
				}
			}
		} else {
			Write-Host ("Table $($rddtable.'DB_REC_MNEM') is in the RDD, but was not found in the physical database")
		}
	}

	# search for tables and columns in the database that are not in the RDD
	foreach ($key in $Sqs.Tables.keys) {
		$rddtable = FindItemInTwoHashes -hash1 $rdd.Tables -hash2 $rdd.TableAliases -key $key
		if ( ![string]::IsNullOrEmpty($rddtable) ) {
			foreach ( $colkey in $sqs.Tables[$key].__columns.keys ) {
				if ( [string]::IsNullOrEmpty( (FindItemInTwoHashes -hash1 $rddtable.__columns -hash2 $rddtable.__columnaliases -key $colkey ) ) ) {
					Write-Host ("Column $colkey of Table $key is in the physical database, but was not found in the RDD")
				}
			}
		} else {
			Write-Host ("Table $key is in the physical database, but was not found in the RDD")
		}
	}
}

Function SqsCompareNormalIndex {
	Param ( $rddIndex, $sqsIndex )

	switch ( $rddIndex.'SOORT_DB_SL' ) {
		'IX' {
			if ( $sqsIndex.'is-unique' ) {
				Write-Host "$($sqsindex.'name') on table $($sqsindex.'TABLE-NAME') should not be an unique index in the physical databse"
			}
			break
		}
		'CK' {
			if ( !$sqsIndex.'is-unique' ) {
				Write-Host "$($sqsindex.'name') on table $($sqsindex.'TABLE-NAME') should be an unique index in the physical databse"
			}
			break
		}
		'PK' {
			if ( !$sqsIndex.'is-primary-key' ) {
				Write-Host "$($sqsindex.'name') on table $($sqsindex.'TABLE-NAME') should be a primary key in the physical databse"
			}
			break
		}
	}

	# create arrays with columns sorted in the order specifed by the index
	# using the @{} ensures that when no columns are found an empty array is returned (damn you powershell)
	# the getenumerator converts the hashes to on object where there the actual object 
	# in the hash is put behind a property Value
	# so accessing member of the object is done by $array[0].Value.Yourproperty
	$sqs_columns = @($sqsIndex.__columns.GetEnumerator() | Sort-Object { $_.Value.'key-ordinal' })
	$rdd_columns = @($rddIndex.__columns.GetEnumerator() | Sort-Object { $_.Value.'ITEM_VNR_SLEUT' })

	$max = ($sqs_columns.count , $rdd_columns.count | Measure-Object -Maximum).Maximum

	for ( $i = 0; $i -lt $max; $i++ ) {
		if ( $i -ge $sqs_columns.count ) {
			# verschil aantal kolommen, rdd heeft er meer dan sqs
			Write-Host "Column $($rdd_columns[$i].Value.'ITEM_INT_MNEM') of index $($rddIndex.'DB_SLEUT_NAAM') of table $($rddIndex[$i].'DB_REC_MNEM') is in the RDD but missing in the Physical database"
		} else {
			if ( $i -ge $rdd_columns.count ) {
				# verschil aantal kolommen, sqs heeft er meer dan rdd
				Write-Host "Column $($sqs_columns[$i].Value.'COLUMN-NAME') of index $($sqs_columns[$i].Value.'INDEX-NAME') of table $($sqs_columns[$i].Value.'TABLE-NAME') is in the Physical database but missing in the RDD"
			} else {
				$rdd_column = $rdd_columns[$i]
				$sqs_column = $sqs_columns[$i]
				# vergelijk de twee kolommen met elkaar
				if ( $rdd_column.Value.'ITEM_INT_MNEM'.Equals($sqs_column.Value.'COLUMN-NAME') ) {
				} else {
					Write-Host "Column $($sqs_column.Value.'COLUMN-NAME') of index $($sqs_column.Value.'INDEX-NAME') of table $($sqs_column.Value.'TABLE-NAME') in the Physical database differs from the Column $($rdd_column.Value.'ITEM_INT_MNEM') in the RDD"
				}
			}
		}
	}
}

Function SqsCompareFkIndex {
	Param ( $rddIndex, $sqsIndex )
}


Function SqsCompareIndexes {
	Param( $rdd, $sqs )

	# PK, CK en IX  wordt gevonden in de sys.indexes
	# PK (Primary KEY)
	# CK is index met unique contraints (CANDIDATE KEY), 
	# IX is alleen index
	# FK komt sys.foreign_keys
	# BK alleen gebruikt bij Oracle database
	# LK wordt gebruikt in Winframe, nog geen idee wat dat type is.

	foreach ( $rddkey in $rdd.Tables.keys ) {
		$rddtable = $rdd.Tables[$rddkey]
		$sqstable = FindItemWithAlias -hash $sqs.Tables -key $rddkey -aliaskey $rddtable.DB_REC_ALIAS
		if ( ![string]::IsNullOrEmpty($sqstable) ) {
			foreach ( $indexkey in $rddTable.__Indexes.keys )
			{
				$rddIndex = $rddTable.__Indexes[$indexkey]

				switch ( $rddIndex.'SOORT_DB_SL' ) {
					{ ('IX', 'CK', 'PK').Contains($_) } {
						$sqsIndex = FindItemWithAlias -hash $sqstable.__indexes -key $indexkey -aliaskey $rddIndex.'DB_SLEUT_ALIAS'
						if ( ![string]::IsNullOrEmpty( $sqsIndex ) ) {
							SqsCompareNormalIndex -rddIndex $rddIndex -sqsIndex $sqsIndex
						} else {
							Write-Host "Index $($indexkey) on table $($rddtable.'DB_REC_MNEM') with columns [$($rddIndex.__columns.keys -join ", ")] is in the RDD, but was not found in the physical database"
						}
						break
					}
					{ ('FK').Contains($_) } {
						$sqsIndex = FindItemWithAlias -hash $sqstable.__foreign_keys -key $indexkey -aliaskey $rddIndex.'DB_SLEUT_ALIAS'
						if ( ![string]::IsNullOrEmpty( $sqsIndex ) ) {
							SqsCompareFkIndex -rddIndex $rddIndex -sqsIndex $sqsIndex
						} else {
							Write-Host "Foreign key $($indexkey) on table $($rddtable.'DB_REC_MNEM') with columns [$($rddIndex.__columns.keys -join ", ")] is in the RDD, but was not found in the physical database"
						}
						break
					}
					{ ('LK').Contains($_) } {
						# ignore LK, legacy winframe type used for Score interface is not physically present in the database
						break
					}
					default {
						Write-Host "The RDD Indextype $($rddIndex.'SOORT_DB_SL') for index $indexkey on table $($rddtable.'DB_REC_MNEM') is not supported by this script"
					}
				}
			}
		} else {
			# table not found -> ignore. This is already detected and reported in Compare tables
		}
	}

	foreach ( $sqskey in $sqs.Tables.keys ) {
		$sqstable = $sqs.Tables[$sqskey]

		$rddtable = FindItemInTwoHashes -hash1 $rdd.Tables -hash2 $rdd.TableAliases -key $sqskey
		if ( ![string]::IsNullOrEmpty($rddtable) ) {
			foreach ( $indexkey in $sqstable.__Indexes.keys ) {
				if ( !($rddtable.__indexes.Contains($indexkey) ) ) {
					$index = $sqstable.__Indexes[$indexkey]
					Write-Host "Index $indexkey on table $($rddtable.'DB_REC_MNEM') with columns [$($index.__columns.keys -join ", ")] was found in the physical database but not in the RDD"
				}
			}
		} else {
			# table not found -> ignore. This is already detected and reported in Compare tables
		}
	}
}

Function CompareSqsWithRddSchema {
	Param (
	$RddServerInstance, 
	$RddDatabase,
	$RddSchema,
	$SqlServerInstance,
	$SqlDatabase,
	$SqlSchema
	)


	Write-Host "Comparing RDD in $RddServerInstance, Database $RddDatabase, RDD Schema $RddSchema"
	Write-Host "With SQL Server $SqlServerInstance, Database $SqlDatabase, SQL Schema $SqlSchema"
	$global:sqs = SqsCollectMetaData -Schema $SqlSchema -SqlServerInstance $SqlServerInstance -Database $SqlDatabase
	$global:rdd = RddCollectMetaData -Schema $RddSchema -SqlServerInstance $RddServerInstance -Database $RddDatabase -DateStamp $(Get-Date)

	SqsCompareTables -rdd $global:rdd -sqs $global:sqs
	SqsCompareIndexes -rdd $global:rdd -sqs $global:sqs
}


CompareSqsWithRddSchema -RddServerInstance "LOCALHOST\SQLEXPRESS" -RddDatabase "RDDDB" -RddSchema "OTP-PD-RDD-SQS" -SqlServerInstance "LOCALHOST\SQLEXPRESS" -SqlDatabase "RDDDB" -SqlSchema "RDD"
#CompareSqsWithRddSchema -RddServerInstance "LOCALHOST\SQLEXPRESS" -RddDatabase "RDDDB" -RddSchema "OTP-ON-BKR-WFR" -SqlServerInstance "LOCALHOST\SQLEXPRESS" -SqlDatabase "RDDDB" -SqlSchema "RDD"
