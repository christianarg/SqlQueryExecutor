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
            if (!Parser.Default.ParseArguments(args, AppController.Instance.Options))
            {
                // TODO: Mensaje amigable 
                Console.WriteLine("Argumentos incorrectos");
                return;
            }

            AppController.Instance.Run();
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
            for (int i = 0; i < AppController.Instance.Options.Threads; i++)
            {
                AppController.Instance.Tasks.Add(Task.Factory.StartNew(() =>
                {
                    var executor = new QueryExecutor
                    {
                        Id = i,
                        ConnString = AppController.Instance.ConnectionString,
                        Query = AppController.Instance.Query
                    };
                    AppController.Instance.QueryExecutors.Add(executor);
                    executor.ExecuteIndefinetly();
                }));
            }
        }

        public void Monitor()
        {
            var consoleOutputService = new ConsoleOutputService();
            var jsonOutput = new JsonFileOutputService();
            var appController = AppController.Instance;
            while (true)
            {
                Thread.Sleep(AppController.ThreadLoopWaitInMiliseconds);
                Console.Clear();

                var result = new ResultData
                {
                    AvgElapsed = appController.QueryExecutors.Average(e => e.LastElapsed.Milliseconds),
                    MaxElapsed = appController.QueryExecutors.Max(e => e.LastElapsed.Milliseconds),
                    MinElapsed = appController.QueryExecutors.Min(e => e.LastElapsed.Milliseconds),
                    ExecutionsInLastCycle = appController.ExecutionsInLastCycle,
                    ExecutionsPerThreadPerSec = 1.0 * AppController.Instance.ExecutionsInLastCycle / AppController.Instance.Options.Threads
                };

                consoleOutputService.WriteOutput(result);
                jsonOutput.WriteOutput(result);

                AppController.Instance.ResetExecutionsInLastCycle();
            }
        }
    }

    public class ProcessService : IWorkerService
    {
        public void Start()
        {
            this.CleanupOutputFilesFromPreviousExecutions();

            for (int i = 0; i < AppController.Instance.Options.Processes; i++)
            {
                var process = Process.Start(new ProcessStartInfo
                {
                    FileName = "sqlqueryexecutor.exe",
                    Arguments = $"-t {AppController.Instance.Options.Threads} -d {AppController.Instance.Options.Db} -c {AppController.Instance.Options.ConnectionPooling}",
                    WindowStyle = ProcessWindowStyle.Minimized
                });
                AppController.Instance.Processes.Add(process);
            }
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);

        }

        public void Monitor()
        {
            Console.Title = "Main process controller";
            Thread.Sleep(1000); // Esperamos un pelin que se rellenen los datos del primer ciclo

            while (true)
            {
                try
                {
                    Thread.Sleep(1000);

                    var resultsFromAllProcesses = ReadResultsFromAllProcesses();

                    Console.WriteLine("Main process controller");
                    Console.Clear();

                    Console.WriteLine($"Database: {AppController.Instance.DatabaseName}");
                    Console.WriteLine($"Total Processes: {AppController.Instance.Options.Processes}");
                    Console.WriteLine($"Threads per Processes: {AppController.Instance.Options.Threads}");
                    Console.WriteLine($"Pooling: {AppController.Instance.Options.ConnectionPooling}");
                    Console.WriteLine($"Avg Elapsed Miliseconds per execution: {resultsFromAllProcesses.Average(d => d.AvgElapsed)}");
                    Console.WriteLine($"Max Elapsed Miliseconds per execution: {resultsFromAllProcesses.Max(d => d.MaxElapsed)}");
                    Console.WriteLine($"Min Elapsed Miliseconds per execution: {resultsFromAllProcesses.Min(d => d.MinElapsed)}");

                    var executionsPerSecond = resultsFromAllProcesses.Sum(d => d.ExecutionsInLastCycle);
                    Console.WriteLine($"Executions per sec: {executionsPerSecond}");
                    Console.WriteLine($"Executions per Process per sec: {(1.0 * executionsPerSecond / resultsFromAllProcesses.Count)}");
                }
                catch (Exception ex)
                {
                    File.AppendAllText("error.log", ex.ToString());
                }
            }
        }

        private void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            AppController.Instance.Processes.ForEach(p => p.Kill());
        }

        private static List<ResultData> ReadResultsFromAllProcesses()
        {
            var filesToRead = GetOutputFilesPath();
            var resultsFromAllProcesses = new List<ResultData>();
            foreach (var file in filesToRead)
            {
                resultsFromAllProcesses.Add(JsonConvert.DeserializeObject<ResultData>(ReadResultFile(file)));
            }

            return resultsFromAllProcesses;
        }

        private static string ReadResultFile(string file)
        {
            using (FileStream logFileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (StreamReader logFileReader = new StreamReader(logFileStream))
            {
                return logFileReader.ReadToEnd();
            }

            //return File.ReadAllText(file);
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

    public class AppController
    {
        private static AppController instance;
        
        public static AppController Instance
        {
            get
            {
                if (instance == null)
                    instance = new AppController();
                return instance;
            }
        }

        public Options Options { get; private set; } = new Options();
        public string Query { get; private set; }
        public string ConnectionString { get; private set; }
        public List<Task> Tasks { get; private set; }
        public List<Process> Processes { get; private set; } = new List<Process>();
        public ConcurrentBag<QueryExecutor> QueryExecutors { get; private set; }
        public string DatabaseName { get; set; }
        // TODOer de manera thread safe http://stackoverflow.com/questions/13181740/c-sharp-thread-safe-fastest-counter
        public int ExecutionsInLastCycle => executionsInLastCycle;
        public const int ThreadLoopWaitInMiliseconds = 1000;

        private int executionsInLastCycle;

        private AppController() { }

        public void Run()
        {
            Initialize();
            Start();
        }

        public void IncrementExecutionsInLastCycle() => Interlocked.Increment(ref executionsInLastCycle);

        public void ResetExecutionsInLastCycle() => Interlocked.Exchange(ref executionsInLastCycle, 0);

        private void Initialize()
        {
            ThreadPool.SetMinThreads(AppController.Instance.Options.Threads, AppController.Instance.Options.Threads);
            AppController.Instance.ConnectionString = ConnectionStringFactory.GetConnString();
            AppController.Instance.DatabaseName = ConnectionStringFactory.GetDatabaseName();
            AppController.Instance.Query = ConfigurationManager.AppSettings["query"];
            AppController.Instance.Tasks = new List<Task>(AppController.Instance.Options.Threads);
            AppController.Instance.QueryExecutors = new ConcurrentBag<QueryExecutor>();
        }

        private void Start()
        {
            var workerService = WorkerFactory.GetWorkerService();
            workerService.Start();
            workerService.Monitor();
        }

        static class WorkerFactory
        {
            public static IWorkerService GetWorkerService()
            {
                if (AppController.Instance.Options.Processes == 1)
                {
                    return new ThreadService();
                }
                return new ProcessService();
            }
        }

        static class ConnectionStringFactory
        {
            public static string GetConnString()
            {
                string connString = ConfigurationManager.ConnectionStrings[AppController.Instance.Options.Db.ToLower()].ConnectionString;

                if (string.IsNullOrEmpty(connString))
                    throw new Exception("ConnectionString not found");

                if (AppController.Instance.Options.ConnectionPooling != "on")
                {
                    connString = $"{connString}Pooling=False;";
                }

                return connString;
            }

            public static string GetDatabaseName()
            {
                var connStringBuilder = new SqlConnectionStringBuilder(GetConnString());
                return connStringBuilder.InitialCatalog;
            }
        }
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
                    AppController.Instance.IncrementExecutionsInLastCycle();
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
        void WriteOutput(ResultData result);
    }

    public class ConsoleOutputService : IOutputService
    {
        public void WriteOutput(ResultData result)
        {
            Console.Clear();
            Console.WriteLine($"Database: {AppController.Instance.DatabaseName}");
            Console.WriteLine($"Total Threads: {AppController.Instance.QueryExecutors.Count}");
            Console.WriteLine($"Pooling: {AppController.Instance.Options.ConnectionPooling}");
            Console.WriteLine($"Avg Elapsed Miliseconds per execution: {result.AvgElapsed}");
            Console.WriteLine($"Max Elapsed Miliseconds per execution: {result.MaxElapsed}");
            Console.WriteLine($"Min Elapsed Miliseconds per execution: {result.MinElapsed}");
            Console.WriteLine($"Executions per sec: {result.ExecutionsInLastCycle}");
            Console.WriteLine($"Executions per Thread per sec: {result.ExecutionsPerThreadPerSec}");
        }
    }

    public class JsonFileOutputService : IOutputService
    {
        public void WriteOutput(ResultData result)
        {
            var outputJson = JsonConvert.SerializeObject(result);

            File.WriteAllText($"{Process.GetCurrentProcess().Id}.output", outputJson);
        }
    }

    public class ResultData
    {
        public double AvgElapsed { get; set; }
        public double MaxElapsed { get; set; }
        public double MinElapsed { get; set; }
        public int ExecutionsInLastCycle { get; set; }
        public double ExecutionsPerThreadPerSec { get; set; }

        public ResultData() { }
    }
}
