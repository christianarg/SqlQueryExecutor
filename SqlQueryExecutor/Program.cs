﻿using CommandLine;
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

            AppController.Instance.Execute();
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

            while (true)
            {
                Thread.Sleep(AppController.ThreadLoopWaitInMiliseconds);
                Console.Clear();

                consoleOutputService.WriteOutput();
                jsonOutput.WriteOutput();

                //Console.WriteLine($"Database: {AppContext.Instance.DatabaseName}");
                //Console.WriteLine($"Total Threads: {AppContext.Instance.QueryExecutors.Count}");
                //Console.WriteLine($"Pooling: {AppContext.Instance.Options.ConnectionPooling}");
                //Console.WriteLine($"Avg Elapsed Miliseconds per execution: {AppContext.Instance.QueryExecutors.Average(e => e.LastElapsed.Milliseconds)}");
                //Console.WriteLine($"Executions per sec: {AppContext.Instance.ExecutionsInLastCycle}");
                //Console.WriteLine($"Executions per Thread per sec: {(1.0 * AppContext.Instance.ExecutionsInLastCycle / AppContext.Instance.Options.Threads)}");

                AppController.Instance.ExecutionsInLastCycle = 0;
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
                });
                AppController.Instance.Processes.Add(process);
            }
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);

        }

        private void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            AppController.Instance.Processes.ForEach(p => p.Kill());
        }

        public void Monitor()
        {
            Thread.Sleep(5000); // Esperamos un pelin que se rellenen los datos del primer ciclo
            Console.Title = "Main process controller";

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
                    Console.WriteLine($"Pooling: {AppController.Instance.Options.ConnectionPooling}");
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
        private AppController() { }
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
        public int ExecutionsInLastCycle { get; set; }
        public const int ThreadLoopWaitInMiliseconds = 1000;

        public void Execute()
        {
            Initialize();
            Start();
        }

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
                    AppController.Instance.ExecutionsInLastCycle++;
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
            Console.WriteLine($"Database: {AppController.Instance.DatabaseName}");
            Console.WriteLine($"Total Threads: {AppController.Instance.QueryExecutors.Count}");
            Console.WriteLine($"Pooling: {AppController.Instance.Options.ConnectionPooling}");
            Console.WriteLine($"Avg Elapsed Miliseconds per execution: {AppController.Instance.QueryExecutors.Sum(e => e.LastElapsed.Milliseconds) / AppController.Instance.QueryExecutors.Count}");
            Console.WriteLine($"Executions per sec: {AppController.Instance.ExecutionsInLastCycle}");
            Console.WriteLine($"Executions per Thread per sec: {(1.0 * AppController.Instance.ExecutionsInLastCycle / AppController.Instance.Options.Threads)}");
        }
    }

    public class JsonFileOutputService : IOutputService
    {
        public void WriteOutput()
        {
            var outputJson = JsonConvert.SerializeObject(new ResultData
            {
                AvgElapsed = AppController.Instance.QueryExecutors.Sum(e => e.LastElapsed.Milliseconds) / AppController.Instance.QueryExecutors.Count,
                ExecutionsInLastCycle = AppController.Instance.ExecutionsInLastCycle
            });

            File.WriteAllText($"{Process.GetCurrentProcess().Id}.output", outputJson);
        }
    }

    public class ResultData
    {
        public int AvgElapsed { get; set; }
        public int ExecutionsInLastCycle { get; set; }
    }
}
