#
#

Import-Module -Name SqlServer


# this function only copies the properties of the Row that have a name that is written in ALL_CAPS
Function FillObject {
	Param ($Row)

	$object = [PSCustomObject]@{ }
	foreach ( $prop in $Row.psobject.properties ) {
		if ( $prop.Name.Equals($prop.Name.ToUpper()) ) {
			$object | Add-Member -MemberType NoteProperty -Name $prop.Name -Value $prop.Value
		}
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
		$Table | Add-Member -MemberType NoteProperty -Name "columns" -Value @{}
		$Tables[$Table.TABLE_NAME] = $Table
	}
	Write-Output $tables
}

Function AddColumnsToTheTableshash {
	Param( $Tables, $ColumnRows )

	foreach ($ColumnRow in $ColumnRows) {
		$Column = FillObject -Row $ColumnRow 
		$Tables[$Column.TABLE_NAME].columns[$Column.COLUMN_NAME] = $Column
	}
	Write-Output $Tables
}

$SqlServerInstance = "SWIFTY\SQLEXPRESS"
$Database = "DatamodelTest"
$TableRows = Invoke-Sqlcmd "select * from INFORMATION_SCHEMA.TABLES" -ServerInstance $SqlServerInstance -Database $Database
$ColumnRows = Invoke-Sqlcmd "select * from INFORMATION_SCHEMA.COLUMNS" -ServerInstance $SqlServerInstance -Database $Database

$tables = CreateTablesHash -TableRows $TableRows
$tables = AddColumnsToTheTableshash -Tables $tables -ColumnRows $ColumnRows


foreach ( $h in $tables.keys ) {
	$table = $tables[$h]
	foreach ( $c in $table.columns.keys ) {
		Write-Host $table.columns[$c]
	}
}
