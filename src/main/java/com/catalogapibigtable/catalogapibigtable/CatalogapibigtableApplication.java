package com.catalogapibigtable.catalogapibigtable;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class CatalogapibigtableApplication {
	private static final String COLUMN_FAMILY = "cf1";
	private static final String COLUMN_QUALIFIER_GREETING = "greeting";
	private static final String COLUMN_QUALIFIER_NAME = "name";
	private static final String ROW_KEY_PREFIX = "rowKey";
	private final String tableId;
	private final BigtableDataClient dataClient;
	private final BigtableTableAdminClient adminClient;
  
	public static void main(String[] args) throws Exception {
  
	//   if (args.length != 2) {
	// 	System.out.println("Missing required project id or instance id");
	// 	return;
	//   }
	  String projectId = "sales-209522";
	  String instanceId = "catalogdb";
  
	  CatalogapibigtableApplication catalogapibigtableApplication = new CatalogapibigtableApplication(projectId, instanceId, "test-table");
	  catalogapibigtableApplication.run();
	}
  
	public CatalogapibigtableApplication(String projectId, String instanceId, String tableId) throws IOException {
	  this.tableId = tableId;
  
	  BigtableDataSettings settings =
		  BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
  
	  dataClient = BigtableDataClient.create(settings);
  
	  BigtableTableAdminSettings adminSettings =
		  BigtableTableAdminSettings.newBuilder()
			  .setProjectId(projectId)
			  .setInstanceId(instanceId)
			  .build();
  
	  adminClient = BigtableTableAdminClient.create(adminSettings);
	}
  
	public void run() throws Exception {
	  createTable();
	  writeToTable();
	  readSingleRow();
	  readSpecificCells();
	  readTable();
	  filterLimitCellsPerCol(tableId);
	  deleteTable();
	  close();
	}
  
	public void close() {
	  dataClient.close();
	  adminClient.close();
	}
  
	public void createTable() {
	  if (!adminClient.exists(tableId)) {
		System.out.println("Creating table: " + tableId);
		CreateTableRequest createTableRequest =
			CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
		adminClient.createTable(createTableRequest);
		System.out.printf("Table %s created successfully%n", tableId);
	  }
	}
  
	public void writeToTable() {
	  try {
		System.out.println("\nWriting some greetings to the table");
		String[] names = {"World", "Bigtable", "Java"};
		for (int i = 0; i < names.length; i++) {
		  String greeting = "Hello " + names[i] + "!";
		  RowMutation rowMutation =
			  RowMutation.create(TableId.of(tableId), ROW_KEY_PREFIX + i)
				  .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_NAME, names[i])
				  .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_GREETING, greeting);
		  dataClient.mutateRow(rowMutation);
		  System.out.println(greeting);
		}
	  } catch (NotFoundException e) {
		System.err.println("Failed to write to non-existent table: " + e.getMessage());
	  }
	}
  
	public Row readSingleRow() {
	  try {
		System.out.println("\nReading a single row by row key");
		Row row = dataClient.readRow(TableId.of(tableId), ROW_KEY_PREFIX + 0);
		System.out.println("Row: " + row.getKey().toStringUtf8());
		for (RowCell cell : row.getCells()) {
		  System.out.printf(
			  "Family: %s    Qualifier: %s    Value: %s%n",
			  cell.getFamily(), cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
		}
		return row;
	  } catch (NotFoundException e) {
		System.err.println("Failed to read from a non-existent table: " + e.getMessage());
		return null;
	  }
	}
  
	public List<RowCell> readSpecificCells() {
	  try {
		System.out.println("\nReading specific cells by family and qualifier");
		Row row = dataClient.readRow(TableId.of(tableId), ROW_KEY_PREFIX + 0);
		System.out.println("Row: " + row.getKey().toStringUtf8());
		List<RowCell> cells = row.getCells(COLUMN_FAMILY, COLUMN_QUALIFIER_NAME);
		for (RowCell cell : cells) {
		  System.out.printf(
			  "Family: %s    Qualifier: %s    Value: %s%n",
			  cell.getFamily(), cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
		}
		return cells;
	  } catch (NotFoundException e) {
		System.err.println("Failed to read from a non-existent table: " + e.getMessage());
		return null;
	  }
	}
  
	public List<Row> readTable() {
	  try {
		System.out.println("\nReading the entire table");
		Query query = Query.create(TableId.of(tableId));
		ServerStream<Row> rowStream = dataClient.readRows(query);
		List<Row> tableRows = new ArrayList<>();
		for (Row r : rowStream) {
		  System.out.println("Row Key: " + r.getKey().toStringUtf8());
		  tableRows.add(r);
		  for (RowCell cell : r.getCells()) {
			System.out.printf(
				"Family: %s    Qualifier: %s    Value: %s%n",
				cell.getFamily(), cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
		  }
		}
		return tableRows;
	  } catch (NotFoundException e) {
		System.err.println("Failed to read a non-existent table: " + e.getMessage());
		return null;
	  }
	}
  
	public void filterLimitCellsPerCol(String tableId) {
	  Filter filter = FILTERS.limit().cellsPerColumn(1);
	  readRowFilter(tableId, filter);
	  readFilter(tableId, filter);
	}
  
	private void readRowFilter(String tableId, Filter filter) {
	  String rowKey =
		  Base64.getEncoder().encodeToString("greeting0".getBytes(StandardCharsets.UTF_8));
	  Row row = dataClient.readRow(TableId.of(tableId), rowKey, filter);
	  printRow(row);
	  System.out.println("Row filter completed.");
	}
  
	private void readFilter(String tableId, Filter filter) {
	  Query query = Query.create(TableId.of(tableId)).filter(filter);
	  ServerStream<Row> rows = dataClient.readRows(query);
	  for (Row row : rows) {
		printRow(row);
	  }
	  System.out.println("Table filter completed.");
	}
  
	public void deleteTable() {
	  System.out.println("\nDeleting table: " + tableId);
	  try {
		adminClient.deleteTable(tableId);
		System.out.printf("Table %s deleted successfully%n", tableId);
	  } catch (NotFoundException e) {
		System.err.println("Failed to delete a non-existent table: " + e.getMessage());
	  }
	}
  
	private static void printRow(Row row) {
	  if (row == null) {
		return;
	  }
	  System.out.printf("Reading data for %s%n", row.getKey().toStringUtf8());
	  String colFamily = "";
	  for (RowCell cell : row.getCells()) {
		if (!cell.getFamily().equals(colFamily)) {
		  colFamily = cell.getFamily();
		  System.out.printf("Column Family %s%n", colFamily);
		}
		String labels =
			cell.getLabels().size() == 0 ? "" : " [" + String.join(",", cell.getLabels()) + "]";
		System.out.printf(
			"\t%s: %s @%s%s%n",
			cell.getQualifier().toStringUtf8(),
			cell.getValue().toStringUtf8(),
			cell.getTimestamp(),
			labels);
	  }
	  System.out.println();
	}

		
}
