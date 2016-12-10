using CommandLine;
using CommandLine.Text;
using Dapper;
using Newtonsoft.Json;
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
        static void Main(string[] args)
        {
            if (!Parser.Default.ParseArguments(args, AppContext.Options))
            {
                // TODO: Mensaje amigable 
                Console.WriteLine("Argumentos incorrectos");
                return;
            }

            InitializeApplicationContext();

            var workerService = GetWorkerService();
            workerService.Start();
            workerService.Monitor();
        }

        private static IWorkerService GetWorkerService()
        {
            if(AppContext.Options.Processes == 1)
            {
                return new ThreadService();
            }
            return new ProcessService();
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

        private static string GetConnString()
        {
            string connString = ConfigurationManager.ConnectionStrings[AppContext.Options.Db.ToLower()].ConnectionString;

            if (string.IsNullOrEmpty(connString))
                throw new Exception("ConnectionString not found");

            if (AppContext.Options.ConnectionPooling != "on")
            {
                connString = $"{connString}Pooling=False;";
            }

            return connString;
        }
    }

    public interface IWorkerService
    {
        void Start();
        void Monitor();
    }
    

    public class ThreadService : IWorkerService
    {
        public void Start()
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

        public void Monitor()
        {
            var consoleOutputService = new ConsoleOutputService();
            var jsonOutput = new JsonFileOutputService();

            while (true)
            {
                Thread.Sleep(AppContext.ThreadLoopWaitInMiliseconds);
                Console.Clear();

                consoleOutputService.WriteOutput();
                jsonOutput.WriteOutput();

                //Console.WriteLine($"Database: {AppContext.DatabaseName}");
                //Console.WriteLine($"Total Threads: {AppContext.QueryExecutors.Count}");
                //Console.WriteLine($"Pooling: {AppContext.Options.ConnectionPooling}");
                //Console.WriteLine($"Avg Elapsed Miliseconds per execution: {AppContext.QueryExecutors.Average(e => e.LastElapsed.Milliseconds)}");
                //Console.WriteLine($"Executions per sec: {AppContext.ExecutionsInLastCycle}");
                //Console.WriteLine($"Executions per Thread per sec: {(1.0 * AppContext.ExecutionsInLastCycle / AppContext.Options.Threads)}");

                AppContext.ExecutionsInLastCycle = 0;
            }
        }
    }

    public class ProcessService : IWorkerService
    {
        public void Start()
        { 
            this.CleanupOutputFilesFromPreviousExecutions();
            for (int i = 0; i < AppContext.Options.Processes; i++)
            {
                var p = Process.Start(new ProcessStartInfo
                {
                    FileName = "sqlqueryexecutor.exe",
                    Arguments = $"-t {AppContext.Options.Threads} -d {AppContext.Options.Db} -c {AppContext.Options.ConnectionPooling}",
                });
            }
        }

        public void Monitor()
        {
            Thread.Sleep(5000); // Esperamos un pelin que se rellenen los datos del primer ciclo

            while (true)
            {
                Console.Title = "Main process controller";
                try
                {
                    Thread.Sleep(1000);
                    Console.Clear();
                    Console.WriteLine("Main process controller");

                    var resultsFromAllProcesses = ReadResultsFromAllProcesses();

                    Console.WriteLine($"Database: {AppContext.DatabaseName}");
                    Console.WriteLine($"Total Processes: {AppContext.Options.Processes}");
                    Console.WriteLine($"Pooling: {AppContext.Options.ConnectionPooling}");
                    Console.WriteLine($"Avg Elapsed Miliseconds per execution: {resultsFromAllProcesses.Average(d => d.AvgElapsed)}");
                    var executionsPerSecond = resultsFromAllProcesses.Sum(d => d.ExecutionsInLastCycle);
                    Console.WriteLine($"Executions per sec: {executionsPerSecond}");
                    Console.WriteLine($"Executions per Thread per sec: {(1.0 * executionsPerSecond / resultsFromAllProcesses.Count)}");
                }
                catch (Exception ex)
                {
                    File.AppendAllText("error.log", ex.ToString());
                }
            }
        }

        private static List<OutputData> ReadResultsFromAllProcesses()
        {
            var filesToRead = GetOutputFilesPath();
            var resultsFromAllProcesses = new List<OutputData>();
            foreach (var file in filesToRead)
            {
                resultsFromAllProcesses.Add(JsonConvert.DeserializeObject<OutputData>(File.ReadAllText(file)));
            }

            return resultsFromAllProcesses;
        }

        private void CleanupOutputFilesFromPreviousExecutions()
        {
            string[] filesToDelete = GetOutputFilesPath();
            foreach (var file in filesToDelete)
            {
                File.Delete(file);
            }
        }

        private static string[] GetOutputFilesPath()
        {
            return Directory.GetFiles(".", "*.output");
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
        // TODO: Acceder de manera thread safe http://stackoverflow.com/questions/13181740/c-sharp-thread-safe-fastest-counter
        public static int ExecutionsInLastCycle { get; set; }
        public const int ThreadLoopWaitInMiliseconds = 1000;
    }

    public class Options
    {
        [Option('d', "db", DefaultValue = "sftpre", HelpText = "Db Key en connectionstring")]
        public string Db { get; set; }
        [Option('c', "conpool", DefaultValue = "on", HelpText = "Pooling;Valores: on|off")]
        public string ConnectionPooling { get; set; }
        [Option('p', "processes", DefaultValue = 1, HelpText = "Processes")]
        public int Processes { get; set; }
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
                    AppContext.ExecutionsInLastCycle++;
                }
            }
            catch (Exception ex)
            {
                File.AppendAllText("errors.log", ex.ToString());
            }
        }
    }

    public interface IOutputService
    {
        void WriteOutput();
    }

    public class ConsoleOutputService : IOutputService
    {
        public void WriteOutput()
        {
            Console.Clear();
            Console.WriteLine($"Database: {AppContext.DatabaseName}");
            Console.WriteLine($"Total Threads: {AppContext.QueryExecutors.Count}");
            Console.WriteLine($"Pooling: {AppContext.Options.ConnectionPooling}");
            Console.WriteLine($"Avg Elapsed Miliseconds per execution: {AppContext.QueryExecutors.Sum(e => e.LastElapsed.Milliseconds) / AppContext.QueryExecutors.Count}");
            Console.WriteLine($"Executions per sec: {AppContext.ExecutionsInLastCycle}");
            Console.WriteLine($"Executions per Thread per sec: {(1.0 * AppContext.ExecutionsInLastCycle / AppContext.Options.Threads)}");
        }
    }

    public class JsonFileOutputService : IOutputService
    {
        public void WriteOutput()
        {
            var outputJson = JsonConvert.SerializeObject(new OutputData
            {
                AvgElapsed = AppContext.QueryExecutors.Sum(e => e.LastElapsed.Milliseconds) / AppContext.QueryExecutors.Count,
                ExecutionsInLastCycle = AppContext.ExecutionsInLastCycle
            });

            File.WriteAllText($"{Process.GetCurrentProcess().Id}.output", outputJson);
        }
    }

    public class OutputData
    {
        public int AvgElapsed { get; set; }
        public int ExecutionsInLastCycle { get; set; }
    }
}
