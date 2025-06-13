using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using SERVIDOR_TCP.Services;

namespace SERVIDOR_TCP
{
    class ServerCli
    {
        static DatabaseService? databaseService;
        
        static async Task Main(string[] args)
        {
            // Inicializar serviço de base de dados
            databaseService = new DatabaseService();
            
            // Inicia o servidor em uma task separada
            var serverTask = Task.Run(() => Servidor.Main(args));

            // Dá um momento para o servidor arrancar
            await Task.Delay(1000);

            // Inicia o menu
            await RunMenu();
        }

        static async Task RunMenu()
        {
            string logFile = "dados_servidor1.txt";

            while (true)
            {                Console.WriteLine("\n--- MENU SERVIDOR ---");
                Console.WriteLine("1. Listar últimos dados recebidos");
                Console.WriteLine("2. Consultar por WAVY, sensor ou intervalo");
                Console.WriteLine("3. Consultar base de dados");
                Console.WriteLine("4. Ver análises realizadas");
                Console.WriteLine("5. Pedir nova análise");
                Console.WriteLine("6. Sair");
                Console.Write("Escolha uma opção: ");
                var op = Console.ReadLine();

                if (op == "1")
                {
                    if (File.Exists(logFile))
                    {
                        var linhas = File.ReadAllLines(logFile).Reverse().Take(20);
                        foreach (var linha in linhas)
                            Console.WriteLine(linha);
                    }
                    else
                    {
                        Console.WriteLine("Nenhum dado encontrado.");
                    }
                }
                else if (op == "2")
                {
                    Console.Write("ID da WAVY (ou Enter para ignorar): ");
                    var wavy = Console.ReadLine();
                    Console.Write("Conteúdo/Sensor (ou Enter para ignorar): ");
                    var sensor = Console.ReadLine();
                    Console.Write("Hora início (HH:mm:ss, ou Enter): ");
                    var inicioStr = Console.ReadLine();
                    Console.Write("Hora fim (HH:mm:ss, ou Enter): ");
                    var fimStr = Console.ReadLine();

                    if (File.Exists(logFile))
                    {
                        var linhas = File.ReadAllLines(logFile);
                        var query = linhas.AsEnumerable();

                        if (!string.IsNullOrWhiteSpace(wavy))
                            query = query.Where(l => l.Contains($"| {wavy} |"));
                        if (!string.IsNullOrWhiteSpace(sensor))
                            query = query.Where(l => l.Contains(sensor));
                        if (TimeSpan.TryParse(inicioStr, out var inicio))
                            query = query.Where(l =>
                            {
                                var partes = l.Split('|');
                                return partes.Length > 0 && TimeSpan.TryParse(partes[0].Trim(), out var t) && t >= inicio;
                            });
                        if (TimeSpan.TryParse(fimStr, out var fim))
                            query = query.Where(l =>
                            {
                                var partes = l.Split('|');
                                return partes.Length > 0 && TimeSpan.TryParse(partes[0].Trim(), out var t) && t <= fim;
                            });

                        foreach (var linha in query.Reverse().Take(50))
                            Console.WriteLine(linha);
                    }
                    else
                    {
                        Console.WriteLine("Nenhum dado encontrado.");
                    }
                }
                else if (op == "3")
                {
                    await ConsultarBaseDados();
                }
                else if (op == "4")
                {
                    await ConsultarAnalises();
                }
                else if (op == "5")
                {
                    // ...existing analysis code...
                }
                else if (op == "6")
                {
                    Environment.Exit(0);
                    break;
                }
            }
        }
        
        static async Task ConsultarBaseDados()
        {
            if (databaseService == null)
            {
                Console.WriteLine("Serviço de base de dados não disponível");
                return;
            }
            
            Console.WriteLine("\n--- CONSULTA BASE DE DADOS ---");
            Console.Write("ID da WAVY (ou Enter para todas): ");
            var wavyId = Console.ReadLine();
            
            Console.Write("Data início (yyyy-MM-dd, ou Enter): ");
            var dataInicioStr = Console.ReadLine();
            DateTime? dataInicio = null;
            if (!string.IsNullOrWhiteSpace(dataInicioStr) && DateTime.TryParse(dataInicioStr, out var di))
                dataInicio = di;
            
            Console.Write("Data fim (yyyy-MM-dd, ou Enter): ");
            var dataFimStr = Console.ReadLine();
            DateTime? dataFim = null;
            if (!string.IsNullOrWhiteSpace(dataFimStr) && DateTime.TryParse(dataFimStr, out var df))
                dataFim = df;
            
            Console.Write("Limite de registros (padrão 20): ");
            var limiteStr = Console.ReadLine();
            int limite = 20;
            if (!string.IsNullOrWhiteSpace(limiteStr) && int.TryParse(limiteStr, out var l))
                limite = l;
            
            var dados = await databaseService.GetWavyDataAsync(
                string.IsNullOrWhiteSpace(wavyId) ? null : wavyId,
                dataInicio,
                dataFim,
                limite
            );
            
            if (dados.Any())
            {
                Console.WriteLine($"\n--- {dados.Count} REGISTOS ENCONTRADOS ---");
                Console.WriteLine("ID\t\tWAVY\t\tTimestamp\t\tHs\tHmax\tTz\tTp");
                Console.WriteLine(new string('-', 80));
                
                foreach (var dado in dados)
                {
                    Console.WriteLine($"{dado.Id}\t\t{dado.WavyId}\t\t{dado.Timestamp:yyyy-MM-dd HH:mm:ss}\t{dado.Hs:F2}\t{dado.Hmax:F2}\t{dado.Tz:F2}\t{dado.Tp:F2}");
                }
            }
            else
            {
                Console.WriteLine("Nenhum registro encontrado");
            }
        }
        
        static async Task ConsultarAnalises()
        {
            if (databaseService == null)
            {
                Console.WriteLine("Serviço de base de dados não disponível");
                return;
            }
            
            Console.WriteLine("\n--- CONSULTA ANÁLISES ---");
            Console.Write("ID da WAVY (ou Enter para todas): ");
            var wavyId = Console.ReadLine();
            
            Console.Write("Data início (yyyy-MM-dd, ou Enter): ");
            var dataInicioStr = Console.ReadLine();
            DateTime? dataInicio = null;
            if (!string.IsNullOrWhiteSpace(dataInicioStr) && DateTime.TryParse(dataInicioStr, out var di))
                dataInicio = di;
            
            var analises = await databaseService.GetAnalysisResultsAsync(
                string.IsNullOrWhiteSpace(wavyId) ? null : wavyId,
                dataInicio,
                30
            );
            
            if (analises.Any())
            {
                Console.WriteLine($"\n--- {analises.Count} ANÁLISES ENCONTRADAS ---");
                Console.WriteLine("ID\t\tWAVY\t\tTimestamp\t\tMédia\t\tDesvio\t\tPadrão\t\tAmostras");
                Console.WriteLine(new string('-', 100));
                
                foreach (var analise in analises)
                {
                    Console.WriteLine($"{analise.Id}\t\t{analise.WavyId}\t\t{analise.AnalysisTimestamp:yyyy-MM-dd HH:mm:ss}\t{analise.Mean:F2}\t\t{analise.StandardDeviation:F2}\t\t{analise.Pattern}\t\t{analise.SampleCount}");
                }
            }
            else
            {
                Console.WriteLine("Nenhuma análise encontrada");
            }
        }
        
        // ...existing code...
    }
}