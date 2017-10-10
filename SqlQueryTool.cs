using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlQueryTool
{
    class SqlQueryTool
    {
        private string ConnectionString { get; set; }
        public int? Timeout { get; set; }

        /// <summary>
        /// This constuctor gives the consumer the option to pass whichever 
        /// Sql Server connection string the consumer would like.
        /// </summary>
        /// <param name="connectionString"></param>
        public SqlQueryTool(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public List<Dictionary<string, object>> Select(string sqlText)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();

            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(sqlText, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                    while (reader.Read())
                    {
                        var dict = new Dictionary<string, object>();
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (reader.GetFieldType(i) == typeof(int))
                            {
                                //dict.Add(columns[i], reader.GetInt32(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (int?)null : reader.GetInt32(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(string))
                            {
                                //dict.Add(columns[i], reader.GetString(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? null : reader.GetString(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(bool))
                            {
                                //dict.Add(columns[i], reader.GetBoolean(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (bool?)null : reader.GetBoolean(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(decimal))
                            {
                                //dict.Add(columns[i], reader.GetDecimal(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (decimal?)null : reader.GetDecimal(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                dict.Add(columns[i], reader.IsDBNull(i) ? (DateTime?)null : reader.GetDateTime(i));
                            }
                        }
                        result.Add(dict);
                    }
                }
            }
            return result;
        }

        public List<Dictionary<string, object>> Select(string sqlText, List<SqlParameter> parameters)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();

            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(sqlText, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                    while (reader.Read())
                    {
                        var dict = new Dictionary<string, object>();
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (reader.GetFieldType(i) == typeof(int))
                            {
                                //dict.Add(columns[i], reader.GetInt32(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (int?)null : reader.GetInt32(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(string))
                            {
                                //dict.Add(columns[i], reader.GetString(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? null : reader.GetString(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(bool))
                            {
                                //dict.Add(columns[i], reader.GetBoolean(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (bool?)null : reader.GetBoolean(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(decimal))
                            {
                                //dict.Add(columns[i], reader.GetDecimal(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (decimal?)null : reader.GetDecimal(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                dict.Add(columns[i], reader.IsDBNull(i) ? (DateTime?)null : reader.GetDateTime(i));
                            }
                        }
                        result.Add(dict);
                    }
                }
            }
            return result;
        }

        public List<Dictionary<string, object>> Select(string sqlText, List<SqlParameter> parameters, SqlConnection connection)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();

            using (SqlCommand command = new SqlCommand(sqlText, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                    while (reader.Read())
                    {
                        var dict = new Dictionary<string, object>();
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (reader.GetFieldType(i) == typeof(int))
                            {
                                //dict.Add(columns[i], reader.GetInt32(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (int?)null : reader.GetInt32(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(string))
                            {
                                //dict.Add(columns[i], reader.GetString(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? null : reader.GetString(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(bool))
                            {
                                //dict.Add(columns[i], reader.GetBoolean(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (bool?)null : reader.GetBoolean(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(decimal))
                            {
                                //dict.Add(columns[i], reader.GetDecimal(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (decimal?)null : reader.GetDecimal(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                dict.Add(columns[i], reader.IsDBNull(i) ? (DateTime?)null : reader.GetDateTime(i));
                            }
                        }
                        result.Add(dict);
                    }
                }
            }
            return result;
        }

        public List<Dictionary<string, object>> Select(string sqlText, List<SqlParameter> parameters, SqlConnection connection, SqlTransaction transaction)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();

            using (SqlCommand command = new SqlCommand(sqlText, connection, transaction))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                    while (reader.Read())
                    {
                        var dict = new Dictionary<string, object>();
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (reader.GetFieldType(i) == typeof(int))
                            {
                                //dict.Add(columns[i], reader.GetInt32(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (int?)null : reader.GetInt32(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(string))
                            {
                                //dict.Add(columns[i], reader.GetString(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? null : reader.GetString(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(bool))
                            {
                                //dict.Add(columns[i], reader.GetBoolean(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (bool?)null : reader.GetBoolean(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(decimal))
                            {
                                //dict.Add(columns[i], reader.GetDecimal(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (decimal?)null : reader.GetDecimal(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                dict.Add(columns[i], reader.IsDBNull(i) ? (DateTime?)null : reader.GetDateTime(i));
                            }
                        }
                        result.Add(dict);
                    }
                }
            }
            return result;
        }

        public List<Dictionary<string, object>> SelectStoredProc(string storedProcName)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();

            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(storedProcName, connection))
            {
                if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                connection.Open();
                command.CommandType = System.Data.CommandType.StoredProcedure;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                    while (reader.Read())
                    {
                        var dict = new Dictionary<string, object>();
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (reader.GetFieldType(i) == typeof(int))
                            {
                                //dict.Add(columns[i], reader.GetInt32(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (int?)null : reader.GetInt32(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(string))
                            {
                                //dict.Add(columns[i], reader.GetString(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? null : reader.GetString(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(bool))
                            {
                                //dict.Add(columns[i], reader.GetBoolean(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (bool?)null : reader.GetBoolean(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(decimal))
                            {
                                //dict.Add(columns[i], reader.GetDecimal(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (decimal?)null : reader.GetDecimal(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                dict.Add(columns[i], reader.IsDBNull(i) ? (DateTime?)null : reader.GetDateTime(i));
                            }
                        }
                        result.Add(dict);
                    }
                }
            }
            return result;
        }

        public List<Dictionary<string, object>> SelectStoredProc(string storedProcName, List<SqlParameter> parameters)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();

            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(storedProcName, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                connection.Open();
                command.CommandType = System.Data.CommandType.StoredProcedure;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                    while (reader.Read())
                    {
                        var dict = new Dictionary<string, object>();
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (reader.GetFieldType(i) == typeof(int))
                            {
                                //dict.Add(columns[i], reader.GetInt32(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (int?)null : reader.GetInt32(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(string))
                            {
                                //dict.Add(columns[i], reader.GetString(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? null : reader.GetString(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(bool))
                            {
                                //dict.Add(columns[i], reader.GetBoolean(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (bool?)null : reader.GetBoolean(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(decimal))
                            {
                                //dict.Add(columns[i], reader.GetDecimal(i));
                                dict.Add(columns[i], reader.IsDBNull(i) ? (decimal?)null : reader.GetDecimal(i));
                            }
                            else if (reader.GetFieldType(i) == typeof(DateTime))
                            {
                                dict.Add(columns[i], reader.IsDBNull(i) ? (DateTime?)null : reader.GetDateTime(i));
                            }
                        }
                        result.Add(dict);
                    }
                }
            }
            return result;
        }

        public int Update(string sqlText, List<SqlParameter> parameters)
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(sqlText, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        public int Update(string sqlText, List<SqlParameter> parameters, SqlConnection connection)
        {
            using (SqlCommand command = new SqlCommand(sqlText, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        public int Update(string sqlText, List<SqlParameter> parameters, SqlConnection connection, SqlTransaction transaction)
        {
            using (SqlCommand command = new SqlCommand(sqlText, connection, transaction))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        public int Insert(string sqlText, List<SqlParameter> parameters)
        {
            using (SqlConnection connection = new SqlConnection(ConnectionString))
            using (SqlCommand command = new SqlCommand(sqlText, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }
                connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        public int Insert(string sqlText, List<SqlParameter> parameters, SqlConnection connection)
        {
            using (SqlCommand command = new SqlCommand(sqlText, connection))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }

                if (connection.State != System.Data.ConnectionState.Open)
                {
                    connection.Open();
                }

                return command.ExecuteNonQuery();
            }
        }

        public int Insert(string sqlText, List<SqlParameter> parameters, SqlConnection connection, SqlTransaction transaction)
        {
            using (SqlCommand command = new SqlCommand(sqlText, connection, transaction))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }

                if (connection.State != System.Data.ConnectionState.Open)
                {
                    connection.Open();
                }

                try
                {
                    return command.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    transaction.Rollback();
                    throw e;
                }
            }
        }

        /// <summary>
        /// Returns the ID of the last item inserted from the sqlText query.
        /// Insert query must end with SELECT SCOPE_IDENTITY().
        /// </summary>
        /// <param name="sqlText"></param>
        /// <param name="parameters"></param>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public int InsertAndReturnId(string sqlText, List<SqlParameter> parameters, SqlConnection connection, SqlTransaction transaction)
        {
            using (SqlCommand command = new SqlCommand(sqlText, connection, transaction))
            {
		if (Timeout != null)
                {
                    command.CommandTimeout = (int)Timeout;
                }
                foreach (var par in parameters)
                {
                    command.Parameters.Add(par);
                }

                if (connection.State != System.Data.ConnectionState.Open)
                {
                    connection.Open();
                }

                try
                {
                    return Convert.ToInt32(command.ExecuteScalar());
                }
                catch (Exception e)
                {
                    transaction.Rollback();
                    throw e;
                }
            }
        }

        private decimal? GetNullableDecimal(SqlDataReader reader, int column)
        {
            return reader.IsDBNull(column) ? (decimal?)null : reader.GetDecimal(column);
        }

        private int? GetNullableInt32(SqlDataReader reader, int column)
        {
            return reader.IsDBNull(column) ? (int?)null : reader.GetInt32(column);
        }

        private bool? GetNullableBool(SqlDataReader reader, int column)
        {
            return reader.IsDBNull(column) ? (bool?)null : reader.GetBoolean(column);
        }

        public T GetObjectFromDictionary<T>(Dictionary<string, object> dict)
        {
            Type type = typeof(T);
            var obj = Activator.CreateInstance(type);

            foreach (var kv in dict)
            {
                type.GetProperty(kv.Key).SetValue(obj, kv.Value);
            }
            return (T)obj;
        }
    }
}
