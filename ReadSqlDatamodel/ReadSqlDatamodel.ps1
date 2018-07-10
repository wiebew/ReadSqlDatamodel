#
#	

Import-Module -Name SqlServer

$SqlServerInstance = "SWIFTY\SQLEXPRESS"
$Database = "RDDDB"
$Schema = "OTP-PD-RDD-SQS-PUB"


# this function only copies the properties of the Row that have a name that is written in ALL_CAPS
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

# returns a hash with tables derived from INFORMATION_SCHEMA.TABLES record from SqlServer
# the tables do not have the columns attached yet
Function CreateTablesHash {
	Param ( $TableRows,  $ColumnRows )
	$Tables = @{}
	foreach ($TableRow in $TableRows) {
		$Table = FillObject -Row $TableRow
		$Table | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Tables[$Table.DB_REC_MNEM] = $Table
	}

	foreach ($ColumnRow in $ColumnRows) {
		$Column = FillObject -Row $ColumnRow 
		$Tables[$Column.DB_REC_MNEM].__columns[$Column.ITEM_INT_MNEM] = $Column
	}

	Write-Output $tables
}


Function CreateIndexeshash {
	Param (  $IndexRows,  $ColumnRows )

	$Indexes = @{}
	foreach ($Row in $IndexRows) {
		$Index = FillObject -Row $Row
		$Index | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$key = $Index.DB_REC_MNEM + "-" + $Index.SOORT_DB_SL + "-" + $Index.DB_SL_VOLG_NR
		$Indexes[] = 
	}


}

Write-Host "Querying RDD Records for schema $Schema"
$TableRows = Invoke-Sqlcmd "select * from RDD.DB_RECORD where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM" -ServerInstance $SqlServerInstance -Database $Database
$TableColumnRows = Invoke-Sqlcmd "select * from RDD.DB_REC_OPB where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, ITEM_INT_MNEM" -ServerInstance $SqlServerInstance -Database $Database
$IndexRows =  Invoke-Sqlcmd "select * from RDD.DB_SLEUTEL where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, SOORT_DB_SL, DB_SL_VOLG_NR" -ServerInstance $SqlServerInstance -Database $Database
$IndexColumnRows =  Invoke-Sqlcmd "select * from RDD.DB_SLEUTEL where DB_SCHEMA_MNEM = `'$Schema`' order by DB_SCHEMA_MNEM, DB_REC_MNEM, SOORT_DB_SL, DB_SL_VOLG_NR" -ServerInstance $SqlServerInstance -Database $Database

Write-Host "Building RDD Tables structure"
$tables = CreateIndexesHash -IndexRows $IndexRows -ColumnRows $IndexColumnRows

Write-Host "Found $($TableRows.count) tables"

