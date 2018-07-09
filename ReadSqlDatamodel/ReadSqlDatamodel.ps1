#
#	

Import-Module -Name SqlServer


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
	Param ( $TableRows )
	$Tables = @{}
	foreach ($TableRow in $TableRows) {
		$Table = FillObject -Row $TableRow
		$Table | Add-Member -MemberType NoteProperty -Name "__columns" -Value @{}
		$Tables[$Table.DB_REC_MNEM] = $Table
	}
	Write-Output $tables
}

Function AddColumnsToTheTableshash {
	Param ( $Tables, $ColumnRows ) 

	foreach ($ColumnRow in $ColumnRows) {
		$Column = FillObject -Row $ColumnRow 
		$Tables[$Column.DB_REC_MNEM].__columns[$Column.ITEM_INT_MNEM] = $Column
	}
	Write-Output $Tables
}

$SqlServerInstance = "SWIFTY\SQLEXPRESS"
$Database = "RDDDB"
$Schema = "OTP-PD-RDD-SQS-PUB"
Write-Host "Querying RDD Records for schema $Schema"
$TableRows = Invoke-Sqlcmd "select * from RDD.DB_RECORD where DB_SCHEMA_MNEM = `'$Schema`'" -ServerInstance $SqlServerInstance -Database $Database
$ColumnRows = Invoke-Sqlcmd "select * from RDD.DB_REC_OPB where DB_SCHEMA_MNEM = `'$Schema`'" -ServerInstance $SqlServerInstance -Database $Database
Write-Host "Building RDD Indexes"

$tables = CreateTablesHash -TableRows $TableRows
$tables = AddColumnsToTheTableshash -Tables $tables -ColumnRows $ColumnRows

Write-Host "Found $($TableRows.count) tables with a total of $($ColumnRows.count) columns"

