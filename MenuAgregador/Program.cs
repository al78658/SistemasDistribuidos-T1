using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

class Launcher
{
    static Dictionary<string, List<string>> bufferWavy = new();
    static Dictionary<string, WavyConfig> wavyConfigs = new();
    static Dictionary<string, WavyStatus> wavyStatus = new();
    static readonly object statusLock = new();
    static readonly object configLock = new();
    static readonly string basePath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\..\AGREGADOR\bin\Debug\net8.0"));

    static void Main(string[] args)
    {
        LoadConfigurations();

        while (true)
        {
            Console.WriteLine("\nMenu do AGREGADOR:");
            Console.WriteLine("1. Listar WAVYs e estados");
            Console.WriteLine("2. Alterar estado de uma WAVY");
            Console.WriteLine("3. Alterar configuração de pré-processamento de uma WAVY");
            Console.WriteLine("4. Adicionar WAVYs (criar ficheiros config_wavyN.txt) ");
            Console.WriteLine("5. Sair");
            Console.Write("Escolha uma opção: ");
            string option = Console.ReadLine();

            switch (option)
            {
                case "1":
                    ListarWavys();
                    break;
                case "2":
                    AlterarEstadoWavy();
                    break;
                case "3":
                    AlterarConfigWavy();
                    break;
                case "4":
                    CriarFicheirosWavy();
                    break;
                case "5":
                    Console.WriteLine("Encerrando o Agregador...");
                    return;
                default:
                    Console.WriteLine("Opção inválida.");
                    break;
            }
        }
    }

    static void LoadConfigurations()
    {
        string statusPath = Path.Combine(basePath, "status.txt");
        string configPrincipalPath = Path.Combine(basePath, "config_wavy.txt");

        // Lê todos os ficheiros config_wavy*.txt
        var configFiles = Directory.GetFiles(basePath, "config_wavy*.txt");
        foreach (var file in configFiles)
        {
            foreach (var line in File.ReadLines(file))
            {
                var parts = line.Split(':');
                if (parts.Length < 4) continue;

                string wavyId = parts[0].ToLower();
                wavyConfigs[wavyId] = new WavyConfig
                {
                    PreProcessamento = parts[1],
                    VolumeDadosEnviar = int.Parse(parts[2]),
                    ServidorAssociado = parts[3]
                };
            }
        }

        // Também lê o ficheiro principal config_wavy.txt
        if (File.Exists(configPrincipalPath))
        {
            foreach (var line in File.ReadLines(configPrincipalPath))
            {
                var parts = line.Split(':');
                if (parts.Length < 4) continue;

                string wavyId = parts[0].ToLower();
                wavyConfigs[wavyId] = new WavyConfig
                {
                    PreProcessamento = parts[1],
                    VolumeDadosEnviar = int.Parse(parts[2]),
                    ServidorAssociado = parts[3]
                };
            }
        }

        if (File.Exists(statusPath))
        {
            foreach (var line in File.ReadLines(statusPath))
            {
                var parts = line.Split(':');
                if (parts.Length < 4) continue;

                string wavyId = parts[0].ToLower();
                var dataTypes = parts[2].Trim('[', ']').Split(',', StringSplitOptions.RemoveEmptyEntries);
                DateTime.TryParseExact(parts[3], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime lastSync);

                wavyStatus[wavyId] = new WavyStatus
                {
                    Status = parts[1],
                    DataTypes = new List<string>(dataTypes),
                    LastSync = lastSync
                };

                if (!bufferWavy.ContainsKey(wavyId))
                    bufferWavy[wavyId] = new List<string>();
            }
        }
    }

    static void ListarWavys()
    {
        Console.WriteLine("\nWAVYs registradas e seus estados:");
        foreach (var wavy in wavyStatus)
        {
            Console.WriteLine($"ID: {wavy.Key}, Estado: {wavy.Value.Status}");
        }
    }

    static void AlterarEstadoWavy()
    {
        Console.Write("\nDigite o ID da WAVY que deseja alterar: ");
        string wavyId = Console.ReadLine()?.Trim().ToLower();

        if (!wavyStatus.ContainsKey(wavyId))
        {
            Console.WriteLine("WAVY não encontrada.");
            return;
        }

        Console.WriteLine("Estados disponíveis: associada, operacao, manutencao, desativada");
        Console.Write("Digite o novo estado: ");
        string novoEstado = Console.ReadLine()?.Trim().ToLower();

        if (novoEstado != "associada" && novoEstado != "operacao" &&
            novoEstado != "manutencao" && novoEstado != "desativada")
        {
            Console.WriteLine("Estado inválido.");
            return;
        }

        AtualizarEstadoWavy(wavyId, novoEstado);
        Console.WriteLine($"Estado da WAVY {wavyId} alterado para '{novoEstado}'.");
    }

    static void AtualizarEstadoWavy(string wavyId, string novoStatus)
    {
        wavyId = wavyId.ToLower();

        List<string> tiposOperacao = new List<string> { "Hs", "Hmax", "Tz", "Tp", "Peak Direction", "SST" };
        List<string> tipos;

        lock (statusLock)
        {
            // Se for para operação, define os tipos normalmente
            if (novoStatus == "operacao")
            {
                tipos = tiposOperacao;
            }
            // Se for para manutenção, mantém os tipos anteriores
            else if (novoStatus == "manutencao" && wavyStatus.ContainsKey(wavyId))
            {
                tipos = new List<string>(wavyStatus[wavyId].DataTypes);
            }
            // Para outros estados (ex: associada, desativada), limpa os tipos
            else
            {
                tipos = new List<string>();
            }

            if (novoStatus == "operacao" && tipos.Count == 0)
            {
                Console.WriteLine("Aviso: Nenhum tipo de dado definido para 'operacao'. Abortando alteração de estado.");
                return;
            }

            Console.WriteLine($"Alterando estado da WAVY '{wavyId}' para '{novoStatus}', tipos de dados: [{string.Join(", ", tipos)}]");

            if (wavyStatus.ContainsKey(wavyId))
            {
                wavyStatus[wavyId].Status = novoStatus;
                wavyStatus[wavyId].LastSync = DateTime.Now;
                wavyStatus[wavyId].DataTypes = tipos;
            }
            else
            {
                wavyStatus[wavyId] = new WavyStatus
                {
                    Status = novoStatus,
                    DataTypes = tipos,
                    LastSync = DateTime.Now
                };
            }
        }

        AtualizarStatusTxt(wavyId, novoStatus);
    }





    static void AtualizarStatusTxt(string wavyId, string novoStatus)
    {
        string statusPath = Path.Combine(basePath, "status.txt");

        lock (statusLock)
        {
            var status = wavyStatus[wavyId];
            string tipos = "[" + string.Join(",", status.DataTypes) + "]";
            string linha = $"{wavyId}:{novoStatus}:{tipos}:{status.LastSync:yyyy-MM-dd HH:mm:ss}";

            var linhas = new List<string>(File.ReadAllLines(statusPath));
            bool linhaAtualizada = false;

            for (int i = 0; i < linhas.Count; i++)
            {
                if (linhas[i].StartsWith(wavyId + ":"))
                {
                    linhas[i] = linha;
                    linhaAtualizada = true;
                    break;
                }
            }

            if (!linhaAtualizada)
            {
                linhas.Add(linha);
            }

            File.WriteAllLines(statusPath, linhas);
        }

        Console.WriteLine($"Estado da WAVY {wavyId} ({novoStatus}) atualizado no ficheiro status.txt");
    }


    static void AlterarConfigWavy()
    {
        Console.Write("\nDigite o ID da WAVY que deseja alterar a configuração: ");
        string wavyId = Console.ReadLine()?.Trim().ToLower();

        if (!wavyConfigs.ContainsKey(wavyId))
        {
            Console.WriteLine("WAVY não encontrada.");
            return;
        }

        Console.WriteLine("Tipos de pré-processamento disponíveis: trim, validar_corrigir, remover_virgulas, nenhum");
        Console.Write("Digite o novo tipo de pré-processamento: ");
        string novoPreProcessamento = Console.ReadLine()?.Trim().ToLower();

        wavyConfigs[wavyId].PreProcessamento = novoPreProcessamento;
        AtualizarConfigWavy(wavyId);
        Console.WriteLine($"Configuração de pré-processamento da WAVY {wavyId} alterada para '{novoPreProcessamento}'.");
    }

    static void AtualizarConfigWavy(string wavyId)
    {
        string configPath = Path.Combine(basePath, "config_wavy.txt");

        lock (configLock)
        {
            var config = wavyConfigs[wavyId];
            string linha = $"{wavyId}:{config.PreProcessamento}:{config.VolumeDadosEnviar}:{config.ServidorAssociado}";

            var linhas = new List<string>(File.ReadAllLines(configPath));
            bool linhaAtualizada = false;

            for (int i = 0; i < linhas.Count; i++)
            {
                if (linhas[i].StartsWith(wavyId + ":"))
                {
                    linhas[i] = linha;
                    linhaAtualizada = true;
                    break;
                }
            }

            if (!linhaAtualizada)
            {
                linhas.Add(linha);
            }

            File.WriteAllLines(configPath, linhas);
        }

        Console.WriteLine($"Configuração da WAVY {wavyId} atualizada no ficheiro config_wavy.txt");
    }

    static void CriarFicheirosWavy()
    {
        Console.Write("\nQuantas WAVYs deseja adicionar? ");
        if (!int.TryParse(Console.ReadLine(), out int quantidade) || quantidade <= 0)
        {
            Console.WriteLine("Número inválido.");
            return;
        }

        string configPrincipalPath = Path.Combine(basePath, "config_wavy.txt");
        string statusPath = Path.Combine(basePath, "status.txt");

        string[] portas = { "5001", "5002", "5003" };

        // Descobre o próximo número de WAVY disponível
        int proximoIndice = 1;
        while (File.Exists(Path.Combine(basePath, $"config_wavy{proximoIndice:D2}.txt")))
        {
            proximoIndice++;
        }

        for (int i = 0; i < quantidade; i++)
        {
            int numeroWavy = proximoIndice + i;
            string id = $"wavy{numeroWavy:D2}";
            string ficheiroWavy = $"config_wavy{numeroWavy:D2}.txt";
            string caminhoFicheiroWavy = Path.Combine(basePath, ficheiroWavy);

            string porta = portas[(numeroWavy - 1) % portas.Length];

            if (!File.Exists(caminhoFicheiroWavy))
            {
                var linhas = new List<string>
            {
                "127.0.0.1",
                porta,
                id,
                "mooloolaba.csv"
            };

                File.WriteAllLines(caminhoFicheiroWavy, linhas);
                Console.WriteLine($"Ficheiro '{ficheiroWavy}' criado com sucesso.");
            }
            else
            {
                Console.WriteLine($"Ficheiro '{ficheiroWavy}' já existe. A ignorar.");
            }

            // Adicionar ao config_wavy.txt
            string linhaConfig = $"{id}:nenhum:5:127.0.0.1";
            var linhasConfig = new List<string>();
            if (File.Exists(configPrincipalPath))
                linhasConfig.AddRange(File.ReadAllLines(configPrincipalPath));

            if (!linhasConfig.Exists(l => l.StartsWith(id + ":")))
            {
                linhasConfig.Add(linhaConfig);
                File.WriteAllLines(configPrincipalPath, linhasConfig);
                Console.WriteLine($"Linha adicionada a config_wavy.txt: {linhaConfig}");
            }

            // Adicionar ao status.txt
            string linhaStatus = $"{id}:associada:[]:{DateTime.Now:yyyy-MM-dd HH:mm:ss}";
            var linhasStatus = new List<string>();
            if (File.Exists(statusPath))
                linhasStatus.AddRange(File.ReadAllLines(statusPath));

            if (!linhasStatus.Exists(l => l.StartsWith(id + ":")))
            {
                linhasStatus.Add(linhaStatus);
                File.WriteAllLines(statusPath, linhasStatus);
                Console.WriteLine($"Entrada adicionada ao status.txt: {linhaStatus}");
            }
        }

        LoadConfigurations(); // Recarrega configs atualizadas
    }
}

class WavyConfig
{
    public string PreProcessamento { get; set; }
    public int VolumeDadosEnviar { get; set; }
    public string ServidorAssociado { get; set; }
}

class WavyStatus
{
    public string Status { get; set; }
    public List<string> DataTypes { get; set; }
    public DateTime LastSync { get; set; }
}
