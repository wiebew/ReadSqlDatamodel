#
#

Import-Module -Name SqlServer

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RddServerInstance = "LOCALHOST\SQLEXPRESS"
$RddDatabase = "RDDDB"
$RddSchema = "OTP-PD-RDD-SQS"

$SqlServerInstance = "LOCALHOST\SQLEXPRESS"
$SqlDatabase = "RDDDB"
$SqlSchema = "RDD"

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

Function RddCreateTablesHash {
	Param ( $TableRows,  $ColumnRows )
	$Tables = @{}
	foreach ($TableRow in $TableRows) {
		$Table = FillObject -Row $TableRow
		$Table | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Table | Add-Member -MemberType NoteProperty -Name "__indexes" -Value @{}
		$Tables[$Table.DB_REC_MNEM] = $Table
	}

	foreach ($ColumnRow in $ColumnRows) {
		$Column = FillObject -Row $ColumnRow
		$Tables[$Column.DB_REC_MNEM].__columns[$Column.ITEM_INT_MNEM] = $Column
	}

	Write-Output $tables
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
		$Schema, $SqlServerInstance, $Database
	)

	Write-Host "Querying RDD Records for schema $Schema"
	# ophalen beperkingen, deze zijn vastgelegd in DB_OPB_BEPERK. Indien het veld BEPERK_TEKST leeg is, dan zit de beperking in de tabel DOMEIN_WAARDE
	# deze is op te halen via DB_OPB_beperk.ITEM_INT_MNEM -> ATTRIBUUT.ATTRIBUUT_MNEM en dan ATTRIBUUT.DOMEIN_ATT -> DOMEIN_WAARDE.DOMEIN_MNEM
	# DOMEIN_WAARDE bevat dan 1 of meer toegestane waarden.
	# we gebruiken joins op attribuut en domein_waarde queries omdat deze tabellen erg groot zijn en een relatief klein aantal records gebruikt wordt (bijv de RDD definitie gebruikt slechts 17 vd ca 21.000 records)
	$BeperkingRows = Invoke-Sqlcmd "select * from RDD.DB_OPB_BEPERK where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, ITEM_INT_MNEM, BEPERK_IDENT" -ServerInstance $SqlServerInstance -Database $Database
	$AttribuutSubsetRows =  Invoke-Sqlcmd "select DISTINCT A.* from RDD.ATTRIBUUT as A join RDD.DB_OPB_beperk AS B ON (A.ATTRIBUUT_MNEM = B.ITEM_INT_MNEM) where B.DB_SCHEMA_MNEM = `'$Schema`' order by ATTRIBUUT_MNEM" -ServerInstance $SqlServerInstance -Database $Database
	$DomeinWaardeSubsetRows =  Invoke-Sqlcmd "select distinct D.* from RDD.DOMEIN_WAARDE as D join RDD.ATTRIBUUT as A ON (D.DOMEIN_MNEM = A.DOMEIN_ATT ) join RDD.DB_OPB_beperk AS B ON (A.ATTRIBUUT_MNEM = B.ITEM_INT_MNEM) where B.DB_SCHEMA_MNEM = `'$Schema`' order by D.DOMEIN_MNEM, D.DOM_WAARDE_VNR" -ServerInstance $SqlServerInstance -Database $Database

	# ophalen tabellen en kolommmen
	$TableRows = Invoke-Sqlcmd "select * from RDD.DB_RECORD where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM" -ServerInstance $SqlServerInstance -Database $Database
	$TableColumnRows = Invoke-Sqlcmd "select * from RDD.DB_REC_OPB where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, ITEM_INT_MNEM" -ServerInstance $SqlServerInstance -Database $Database

	# ophalen indexen en kolommmen
	$IndexRows =  Invoke-Sqlcmd "select * from RDD.DB_SLEUTEL where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, SOORT_DB_SL, DB_SL_VOLG_NR" -ServerInstance $SqlServerInstance -Database $Database
	$IndexColumnRows =  Invoke-Sqlcmd "select * from RDD.DB_SLEUT_OPB where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, SOORT_DB_SL, DB_SL_VOLG_NR" -ServerInstance $SqlServerInstance -Database $Database

	# ophalen toevoeginen
	# eigenlijk custom scripts, hiervan is lastig te valideren of ze zijn uitegvoerd op de target database.
	$ToevoegingRows =  Invoke-Sqlcmd "select * from RDD.DB_SCHEMA_TOEV where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, SCH_TOEV_TEKST" -ServerInstance $SqlServerInstance -Database $Database

	Write-Host "Building RDD Beperkingen structure"
	$beperkingen = RddCreateBeperkingenHash -BeperkingRows $BeperkingRows
	$attributenUsed = RddCreateAttributenHash -AttribuutRows $AttribuutSubsetRows
	$domeinenUsed = RddCreateDomeinenHash -DomeinWaardeRows $DomeinWaardeSubsetRows
	Write-Host "Building RDD Tables structure"
	$tables = RddCreateTablesHash -TableRows $TableRows -ColumnRows $TableColumnRows
	Write-Host "Building RDD Indexes structure"
	$indexes = RddCreateIndexesHash -IndexRows $IndexRows -ColumnRows $IndexColumnRows -Tables $tables
	Write-Host "Building RDD Toevoegingen structure"
	$toevoegingen = RddCreateToevoegingenList -ToevoegingRows $ToevoegingRows

	Write-Host "Found $($beperkingen.count) beperkingen, $($TableRows.count) tables, $($IndexRows.count) indexes, $($ToevoegingRows.count) toevoegingen"

	Write-Output @{ Tables = $tables; Indexes = $indexes; Toevoegingen = $toevoegingen; Beperkingen = $beperkingen; Attributen = $attributenUsed; Domeimen = $domeinenUsed }
}

Function SqsCreateTablesHash {
	Param ( $TableRows,  $ColumnRows )
	$Tables = @{}

	$table_lookup = @{}

	foreach ($TableRow in $TableRows) {
		$Table = FillObjectDashNames -Row $TableRow
		$Table | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Table | Add-Member -MemberType NoteProperty -Name "__indexes" -Value @{}
		$table_lookup[$Table.'object-id'] = $Table
		$Tables[$Table.name.replace("_","-")] = $Table
	}

	foreach ($ColumnRow in $ColumnRows) {
		$Column = FillObjectDashNames -Row $ColumnRow
		$table_lookup[$Column.'object-id'].__columns[$column.name.replace("_","-")] = $Column
	}

	Write-Output $tables
}

$rdd = RddCollectMetaData -Schema $RddSchema -SqlServerInstance $RddServerInstance -Database $RddDatabase

$TableRows = Invoke-Sqlcmd "select * from sys.tables where schema_name(schema_id) = '$SqlSchema'" -ServerInstance $SqlServerInstance -Database $SqlDatabase
$ColumnRows = Invoke-Sqlcmd "SELECT t.name as __TABLE_NAME, c.* FROM sys.columns c JOIN sys.tables t ON (t.object_id = c.object_id) where schema_name(t.schema_id) = '$SqlSchema'" -ServerInstance $SqlServerInstance -Database $SqlDatabase
$SqlTables = SqsCreateTablesHash -TableRows $TableRows -ColumnRows $ColumnRows

$sqs = @{ Tables = $SqlTables };

foreach ($key in $rdd.Tables.keys) {
	if ( $sqs.Tables.Contains($key) ) {
		$rdd_columns =  $rdd.Tables[$key].__columns 
		$sql_columns = $sqs.Tables[$key].__columns 

		foreach ( $colkey in $rdd_columns.keys ) {
			if ( $sql_columns.Contains($colkey) ) {
			} else {
				Write-Host ("Column $colkey of Table $key is in the RDD, but was not found in the physical database")
			}
		}
	} else {
		Write-Host ("Table $key is in the RDD, but was not found in the physical database")
	}
}

foreach ($key in $SqlTables.keys) {
	$rdd_table =  $SqlTables[$key]

	if ( $rdd.Tables.Contains($key) ) {
		$rdd_columns =  $rdd.Tables[$key].__columns 
		$sql_columns = $sqs.Tables[$key].__columns 

		foreach ( $colkey in $sql_columns.keys ) {
			if ( $rdd_columns.Contains($colkey) ) {
			} else {
				Write-Host ("Column $colkey of Table $key is in the physical database, but was not found in the RDD")
			}
		}

	} else {
		Write-Host ("Table $key is in the physical database, but was not found in the RDD")
	}
}