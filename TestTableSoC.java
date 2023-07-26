import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class TestTableSoC {
    public static void main(String[] args) {
        // Database connection details
        String url = "jdbc:mysql://52.66.220.229:6603/budibase?allowPublicKeyRetrieval=true&useSSL=false";
        String username = "budibase";
        String password = "Budibase@23";

        try {
            // Get the date input from the user
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            System.out.print("Enter the date (yyyy-MM-dd): ");
            String dateStr = new Scanner(System.in).nextLine();
            Date date = dateFormat.parse(dateStr);

            // Connect to the database
            Connection connection = DriverManager.getConnection(url, username, password);

            // Query to retrieve the id and socdate columns from arbcase_ext table for the given date
            String selectQuery = "SELECT id, socdate FROM arbcase_ext WHERE DATE(socdate) = ?";

            // Query to insert new rows into the test table
            String insertQuery = "INSERT INTO test (case_id, NoA_date, SoC_date, IO_date) VALUES (?, ?, ?, ?)";

            // Query to update existing rows in the test table
            String updateQuery = "UPDATE test SET SoC_date = ? WHERE case_id = ?";

            // Prepare the statements
            PreparedStatement selectStatement = connection.prepareStatement(selectQuery);
            PreparedStatement insertStatement = connection.prepareStatement(insertQuery);
            PreparedStatement updateStatement = connection.prepareStatement(updateQuery);

            // Set the date parameter for the select query
            selectStatement.setDate(1, new java.sql.Date(date.getTime()));

            // Execute the select query
            ResultSet resultSet = selectStatement.executeQuery();

            // Iterate over the result set and insert/update the values in the test table
            while (resultSet.next()) {
                // Get the id and socdate values from the result set
                int id = resultSet.getInt("id");
                Date socDate = resultSet.getDate("socdate");

                // Check if case_id already exists in the test table
                boolean exists = checkIfExists(connection, id);

                if (exists) {
                    // Case_id already exists, update the SoC_date in the test table
                    updateStatement.setTimestamp(1, new Timestamp(socDate.getTime()));
                    updateStatement.setInt(2, id);
                    updateStatement.executeUpdate();
                } else {
                    // Case_id does not exist, insert a new row in the test table
                    Date noaDate = getNoADate(connection, id);
                    Date ioDate = getIoDate(connection, id);

                    insertStatement.setInt(1, id);
                    insertStatement.setTimestamp(2, new Timestamp(noaDate.getTime()));
                    insertStatement.setTimestamp(3, new Timestamp(socDate.getTime()));

                    if (ioDate != null) {
                        insertStatement.setTimestamp(4, new Timestamp(ioDate.getTime()));
                    } else {
                        insertStatement.setNull(4, Types.TIMESTAMP);
                    }

                    insertStatement.executeUpdate();
                }
            }

            // Close the result set and statements
            resultSet.close();
            selectStatement.close();
            insertStatement.close();
            updateStatement.close();

            // Close the connection
            connection.close();

            System.out.println("Data populated/updated successfully in the 'test' table.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean checkIfExists(Connection connection, int caseId) throws SQLException {
        String query = "SELECT case_id FROM test WHERE case_id = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, caseId);
        ResultSet resultSet = statement.executeQuery();
        boolean exists = resultSet.next();
        resultSet.close();
        statement.close();
        return exists;
    }

    private static Date getNoADate(Connection connection, int caseId) throws SQLException {
        String query = "SELECT accepted_at FROM arbcase WHERE id = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, caseId);
        ResultSet resultSet = statement.executeQuery();
        Date noaDate = null;
        if (resultSet.next()) {
            noaDate = resultSet.getTimestamp("accepted_at");
        }
        resultSet.close();
        statement.close();
        return noaDate;
    }

    private static Date getIoDate(Connection connection, int caseId) throws SQLException {
        String query = "SELECT date, event FROM arbitration_date WHERE id = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, caseId);
        ResultSet resultSet = statement.executeQuery();
        Date ioDate = null;
        if (resultSet.next()) {
            Timestamp date = resultSet.getTimestamp("date");
            String event = resultSet.getString("event");
            if (event.equals("intrm_ext")) {
                ioDate = new Date(date.getTime());
            }
        }
        resultSet.close();
        statement.close();
        return ioDate;
    }
}