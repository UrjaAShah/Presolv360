import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class TestTableNoA{
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

            // Query to retrieve the id and accepted_at columns from arbcase table for the given date
            String selectQuery = "SELECT id, accepted_at FROM arbcase WHERE DATE(accepted_at) = ?";

            // Query to insert new rows into the test table
            String insertQuery = "INSERT INTO test (case_id, NoA_date, SoC_date, IO_date) VALUES (?, ?, ?, ?)";

            // Query to update existing rows in the test table
            String updateQuery = "UPDATE test SET NoA_date = ?, SoC_date = ?, IO_date = ? WHERE case_id = ?";

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
                // Get the id and accepted_at values from the result set
                int id = resultSet.getInt("id");
                Date acceptedAt = resultSet.getTimestamp("accepted_at");

                // Check if case_id already exists in the test table
                boolean exists = checkIfExists(connection, id);

                if (exists) {
                    // Case_id already exists, update the row in the test table
                    updateStatement.setTimestamp(1, new Timestamp(acceptedAt.getTime()));
                    updateStatement.setTimestamp(2, getSocDate(connection, id));
                    updateStatement.setTimestamp(3, getIoDate(connection, id));
                    updateStatement.setInt(4, id);
                    updateStatement.executeUpdate();
                } else {
                    // Case_id does not exist, insert a new row in the test table
                    insertStatement.setInt(1, id);
                    insertStatement.setTimestamp(2, new Timestamp(acceptedAt.getTime()));
                    insertStatement.setTimestamp(3, getSocDate(connection, id));
                    insertStatement.setTimestamp(4, getIoDate(connection, id));
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

    private static Timestamp getSocDate(Connection connection, int caseId) throws SQLException {
        String query = "SELECT socdate FROM arbcase_ext WHERE id = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, caseId);
        ResultSet resultSet = statement.executeQuery();
        Timestamp socDate = null;
        if (resultSet.next()) {
            socDate = resultSet.getTimestamp("socdate");
        }
        resultSet.close();
        statement.close();
        return socDate;
    }

    private static Timestamp getIoDate(Connection connection, int caseId) throws SQLException {
        String query = "SELECT date, event FROM arbitration_date WHERE id = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, caseId);
        ResultSet resultSet = statement.executeQuery();
        Timestamp ioDate = null;
        if (resultSet.next()) {
            Timestamp date = resultSet.getTimestamp("date");
            String event = resultSet.getString("event");
            if (event.equals("intrm_ext")) {
                ioDate = new Timestamp(date.getTime());
            }
        }
        resultSet.close();
        statement.close();
        return ioDate;
    }
}