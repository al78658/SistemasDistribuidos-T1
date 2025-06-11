using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace SERVIDOR_TCP
{
    class ServerCli
    {
        static async Task Main(string[] args)
        {
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
            {
                Console.WriteLine("\n--- MENU SERVIDOR ---");
                Console.WriteLine("1. Listar últimos dados recebidos");
                Console.WriteLine("2. Consultar por WAVY, sensor ou intervalo");
                Console.WriteLine("3. Pedir nova análise");
                Console.WriteLine("4. Sair");
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
                    Console.WriteLine("Parâmetros para nova análise:");
                    Console.Write("ID da WAVY: ");
                    var wavy = Console.ReadLine();
                    Console.Write("Conteúdo/Sensor: ");
                    var sensor = Console.ReadLine();
                    Console.Write("Hora início (HH:mm:ss): ");
                    var inicioStr = Console.ReadLine();
                    Console.Write("Hora fim (HH:mm:ss): ");
                    var fimStr = Console.ReadLine();

                    Console.WriteLine($"[SIMULAÇÃO] Nova análise pedida para WAVY={wavy}, Sensor={sensor}, De={inicioStr} Até={fimStr}");
                    // Aqui podes chamar o teu serviço de análise, se existir
                }
                else if (op == "4")
                {
                    Environment.Exit(0);
                    break;
                }
            }
        }
    }
}