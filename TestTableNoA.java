import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class TestTableIO {
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
            try (Connection connection = DriverManager.getConnection(url, username, password)) {

                // Query to retrieve the id and date columns from arbitration_date table for the given date and event
                String selectQuery = "SELECT id, date FROM arbitration_date WHERE DATE(date) = ? AND event = 'intrm_ext'";

                // Query to insert new rows into the test table
                String insertQuery = "INSERT INTO test (case_id, NoA_date, SoC_date, IO_date) VALUES (?, ?, ?, ?)";

                // Query to update existing rows in the test table
                String updateQuery = "UPDATE test SET IO_date = ? WHERE case_id = ?";

                // Prepare the statements
                try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery);
                     PreparedStatement insertStatement = connection.prepareStatement(insertQuery);
                     PreparedStatement updateStatement = connection.prepareStatement(updateQuery)) {

                    // Set the date parameter for the select query
                    selectStatement.setDate(1, new java.sql.Date(date.getTime()));

                    // Execute the select query
                    try (ResultSet resultSet = selectStatement.executeQuery()) {

                        // Iterate over the result set
                        while (resultSet.next()) {
                            // Get the id and date values from the result set
                            int id = resultSet.getInt("id");
                            Date ioDate = resultSet.getDate("date");

                            // Check if case_id already exists in the test table
                            boolean exists = checkIfExists(connection, id);

                            if (exists) {
                                // Case_id already exists, update the row in the test table
                                updateStatement.setTimestamp(1, new Timestamp(ioDate.getTime()));
                                updateStatement.setInt(2, id);
                                updateStatement.executeUpdate();
                            } else {
                                // Case_id does not exist, insert a new row in the test table
                                Date noaDate = getNoADate(connection, id);
                                Date socDate = getSocDate(connection, id);

                                insertStatement.setInt(1, id);
                                insertStatement.setTimestamp(2, noaDate != null ? new Timestamp(noaDate.getTime()) : null);
                                insertStatement.setTimestamp(3, socDate != null ? new Timestamp(socDate.getTime()) : null);
                                insertStatement.setTimestamp(4, new Timestamp(ioDate.getTime()));
                                insertStatement.executeUpdate();
                            }

                            // Update NoA_date column in the test table
                            Date noaDate = getNoADate(connection, id);
                            if (noaDate != null) {
                                String updateNoADateQuery = "UPDATE test SET NoA_date = ? WHERE case_id = ?";
                                try (PreparedStatement updateNoADateStatement = connection.prepareStatement(updateNoADateQuery)) {
                                    updateNoADateStatement.setTimestamp(1, new Timestamp(noaDate.getTime()));
                                    updateNoADateStatement.setInt(2, id);
                                    updateNoADateStatement.executeUpdate();
                                }
                            }

                            // Update SoC_date column in the test table
                            Date socDate = getSocDate(connection, id);
                            if (socDate != null) {
                                String updateSocDateQuery = "UPDATE test SET SoC_date = ? WHERE case_id = ?";
                                try (PreparedStatement updateSocDateStatement = connection.prepareStatement(updateSocDateQuery)) {
                                    updateSocDateStatement.setTimestamp(1, new Timestamp(socDate.getTime()));
                                    updateSocDateStatement.setInt(2, id);
                                    updateSocDateStatement.executeUpdate();
                                }
                            }
                        }
                    }
                }

                System.out.println("Data populated/updated successfully in the 'test' table.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean checkIfExists(Connection connection, int caseId) throws SQLException {
        String query = "SELECT case_id FROM test WHERE case_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, caseId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private static Date getNoADate(Connection connection, int id) throws SQLException {
        String query = "SELECT accepted_at FROM arbcase WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getDate("accepted_at");
                }
            }
        }
        return null;
    }

    private static Date getSocDate(Connection connection, int id) throws SQLException {
        String query = "SELECT socdate FROM arbcase_ext WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getDate("socdate");
                }
            }
        }
        return null;
    }
}