using CommandLine;
using CommandLine.Text;
using Dapper;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SqlQueryExecutor
{
    class Program
    {
        public static int executionsInTheLastCycle = 0;

        static void Main(string[] args)
        {
            if (!Parser.Default.ParseArguments(args, AppContext.Options))
            {
                // TODO: Mensaje amigable 
                Console.WriteLine("Argumentos incorrectos");
                return;
            }

            InitializeApplicationContext();

            StartQueries();

            MonitorQueries();
        }

        private static void InitializeApplicationContext()
        {
            ThreadPool.SetMinThreads(AppContext.Options.Threads, AppContext.Options.Threads);
            AppContext.ConnectionString = GetConnString();
            AppContext.Query = ConfigurationManager.AppSettings["query"];
            AppContext.Tasks = new List<Task>(AppContext.Options.Threads);
            AppContext.QueryExecutors = new ConcurrentBag<QueryExecutor>();
            var connStringBuilder = new SqlConnectionStringBuilder(AppContext.ConnectionString);
            AppContext.DatabaseName = connStringBuilder.InitialCatalog;
        }

        private static void StartQueries()
        {
            for (int i = 0; i < AppContext.Options.Threads; i++)
            {
                AppContext.Tasks.Add(Task.Factory.StartNew(() =>
                {
                    var executor = new QueryExecutor
                    {
                        Id = i,
                        ConnString = AppContext.ConnectionString,
                        Query = AppContext.Query
                    };
                    AppContext.QueryExecutors.Add(executor);
                    executor.ExecuteIndefinetly();
                }));
            }
        }

        private static void MonitorQueries()
        {
            while (true)
            {
                Thread.Sleep(1000);
                Console.Clear();
                Console.WriteLine($"Database: {AppContext.DatabaseName}");
                Console.WriteLine($"Total Threads: {AppContext.QueryExecutors.Count}");
                Console.WriteLine($"Pooling: {AppContext.Options.Pooling}");
                Console.WriteLine($"Avg Elapsed Miliseconds per execution: {AppContext.QueryExecutors.Sum(e => e.LastElapsed.Milliseconds) / AppContext.QueryExecutors.Count}");
                Console.WriteLine($"Executions per sec: {Program.executionsInTheLastCycle}");
                Console.WriteLine($"Executions per Thread per sec: {(1.0 * Program.executionsInTheLastCycle / AppContext.Options.Threads)}");
                Program.executionsInTheLastCycle = 0;
            }
        }

        private static string GetConnString()
        {
            string connString = ConfigurationManager.ConnectionStrings[AppContext.Options.Db.ToLower()].ConnectionString;

            if (string.IsNullOrEmpty(connString))
                throw new Exception("ConnectionString not found");

            if (AppContext.Options.Pooling != "on")
            {
                connString = $"{connString}Pooling=False;";
            }

            return connString;
        }
    }

    public class AppContext
    {
        public static Options Options { get; set; } = new Options();
        public static string Query { get; set; }
        public static string ConnectionString { get; set; }
        public static List<Task> Tasks { get; set; }
        public static ConcurrentBag<QueryExecutor> QueryExecutors { get; set; }
        public static string DatabaseName { get; set; }
    }

    public class Options
    {
        [Option('d', "db", DefaultValue = "sftpre", HelpText = "Db Key en connectionstring")]
        public string Db { get; set; }
        [Option('p', "pool", DefaultValue = "on", HelpText = "Pooling;Valores: on|off")]
        public string Pooling { get; set; }
        [Option('t', "theads", DefaultValue = 1, HelpText = "Threads")]
        public int Threads { get; set; }
    }

    public class QueryExecutor
    {
        public int Id { get; set; }
        public string ConnString { get; set; }
        public string Query { get; set; }
        public TimeSpan LastElapsed { get; set; }

        public void ExecuteIndefinetly()
        {
            while (true)
            {
                Execute();
            }
        }

        public void Execute()
        {
            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                using (var con = new SqlConnection(ConnString))
                {
                    con.Query(Query).ToList();

                    stopwatch.Stop();
                    this.LastElapsed = stopwatch.Elapsed;
                    Program.executionsInTheLastCycle++;
                }
            }
            catch (Exception ex)
            {
                File.AppendAllText("errors.log", ex.ToString());
            }
        }
    }
}
