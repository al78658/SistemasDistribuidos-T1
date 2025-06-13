using System;
using System.Globalization;
using System.IO;
using System.Linq;

class ServerCli
{
    static void Main(string[] args)
    {
        string logFile = "dados_servidor1.txt";
        string analiseFile = "resultados_analise.txt"; // Ficheiro para guardar resultados das análises

        while (true)
        {
            Console.WriteLine("\n--- MENU SERVIDOR ---");
            Console.WriteLine("1. Listar últimos dados recebidos");
            Console.WriteLine("2. Visualizar resultados das análises");
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
                if (File.Exists(analiseFile))
                {
                    var linhas = File.ReadAllLines(analiseFile).Reverse().Take(20);
                    Console.WriteLine("\n--- Resultados das Análises ---");
                    foreach (var linha in linhas)
                        Console.WriteLine(linha);
                }
                else
                {
                    Console.WriteLine("Nenhum resultado de análise encontrado.");
                }
            }
            else if (op == "3")
            {
                Console.WriteLine("Parâmetros para nova análise:");
                Console.Write("ID da WAVY: ");
                var wavy = Console.ReadLine();
                Console.Write("Sensor/Conteúdo: ");
                var sensor = Console.ReadLine();
                Console.Write("Hora início (HH:mm:ss): ");
                var inicioStr = Console.ReadLine();
                Console.Write("Hora fim (HH:mm:ss): ");
                var fimStr = Console.ReadLine();

                // Simulação de análise: filtra dados do log e calcula média
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

                    // Extrai valores numéricos para análise
                    var valores = query
                        .SelectMany(l => l.Split('|').Skip(2).SelectMany(s => s.Split(';', ',', ' ')))
                        .Select(s => {
                            double v;
                            return double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out v) ? (double?)v : null;
                        })
                        .Where(v => v.HasValue)
                        .Select(v => v.Value)
                        .ToList();

                    if (valores.Count > 0)
                    {
                        var media = valores.Average();
                        var stddev = Math.Sqrt(valores.Select(v => Math.Pow(v - media, 2)).Average());
                        var resultado = $"[{DateTime.Now:HH:mm:ss}] WAVY={wavy}, Sensor={sensor}, Média={media:F2}, StdDev={stddev:F2}, Intervalo={inicioStr}-{fimStr}";
                        Console.WriteLine("[RESULTADO] " + resultado);
                        File.AppendAllText(analiseFile, resultado + Environment.NewLine);
                    }
                    else
                    {
                        Console.WriteLine("Nenhum valor numérico encontrado para análise.");
                    }
                }
                else
                {
                    Console.WriteLine("Nenhum dado encontrado para análise.");
                }
            }
            else if (op == "4")
            {
                break;
            }
        }
    }
}
