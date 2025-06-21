package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Empresa struct {
	CNPJ          string  `json:"cnpj"`
	RazaoSocial   string  `json:"razao_social"`
	NomeFantasia  string  `json:"nome_fantasia"`
	CapitalSocial float64 `json:"capital_social"`
	Logradouro    string  `json:"logradouro"`
	Municipio     string  `json:"municipio"`
	UF            string  `json:"uf"`
	Cep           string  `json:"cep"`
}

var (
	client         = &http.Client{Timeout: 30 * time.Second}
	processedCNPJs = make(map[string]time.Time)
	fileMutex      sync.Mutex
)

func main() {
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/", indexHandler)

	fmt.Println("Servidor iniciado na porta 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, `
	<html>
		<head><title>Busca de Empresas</title></head>
		<body>
			<h1>Upload de Arquivo CSV</h1>
			<form action="/upload" method="post" enctype="multipart/form-data">
				<input type="file" name="file" accept=".csv" required>
				<button type="submit">Enviar</button>
			</form>
		</body>
	</html>
	`)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, "Erro ao analisar o formulário: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Erro ao obter o arquivo: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	outputFileName := "empresas_capital_maior_50000_" + time.Now().Format("20060102_150405") + ".csv"
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		http.Error(w, "Erro ao criar arquivo de saída: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer outputFile.Close()

	outputCSV := csv.NewWriter(outputFile)
	defer outputCSV.Flush()

	// Escrever cabeçalho
	if err := outputCSV.Write([]string{
		"CNPJ",
		"RazaoSocial",
		"NomeFantasia",
		"CapitalSocial",
		"Logradouro",
		"Municipio",
		"UF",
		"CEP",
		"DDD", 
		"Telefone",
		"Email",
	}); err != nil {
		http.Error(w, "Erro ao escrever cabeçalho: "+err.Error(), http.StatusInternalServerError)
		return
	}

	reader := csv.NewReader(file)
	reader.Comma = ';'
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		http.Error(w, "Erro ao ler o arquivo CSV: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Canal para controlar o processamento
	done := make(chan bool)

	go func() {
		log.Println("Iniciando processamento do arquivo:", header.Filename)
		processRecords(records, outputCSV)
		log.Println("Processamento concluído. Resultados salvos em:", outputFileName)
		done <- true
	}()

	// Esperar o processamento terminar antes de retornar a resposta
	<-done

	fmt.Fprintf(w, "Arquivo %s processado com sucesso. Resultados salvos em: %s", header.Filename, outputFileName)
}

func processRecords(records [][]string, outputCSV *csv.Writer) {
	for _, record := range records {
		if len(record) < 28 {
			continue
		}

		// Extrair CNPJ
		cnpj := strings.Trim(record[0], `" `) + strings.Trim(record[1], `" `) + strings.Trim(record[2], `" `)

		if !validarCNPJ(cnpj) {
			continue
		}

		// Verificar cache
		fileMutex.Lock()
		if lastProcessed, exists := processedCNPJs[cnpj]; exists && time.Since(lastProcessed) < 2*time.Hour {
			fileMutex.Unlock()
			continue
		}
		fileMutex.Unlock()

		// Consultar API
		empresa, err := consultarCNPJ(cnpj)
		if err != nil {
			log.Printf("Erro ao consultar CNPJ %s: %v", cnpj, err)
			continue
		}

		// Atualizar cache
		fileMutex.Lock()
		processedCNPJs[cnpj] = time.Now()
		fileMutex.Unlock()

		// Verificar capital social
		if empresa.CapitalSocial > 50000 {
			// Extrair telefone e email do *arquivo CSV de entrada*
			ddd := strings.Trim(record[21], `" `)   
            telefone := strings.Trim(record[22], `" `) // Índice para o telefone no seu CSV
            email := strings.Trim(record[27], `" `)   // **Corrigido: Índice para o e-mail no seu CSV**

            // Escrever no arquivo com mutex
            fileMutex.Lock()
            if err := outputCSV.Write([]string{
                cnpj,
                empresa.RazaoSocial,
                empresa.NomeFantasia,
                strconv.FormatFloat(empresa.CapitalSocial, 'f', 2, 64),
                empresa.Logradouro,
                empresa.Municipio,
                empresa.UF,
                empresa.Cep,
				ddd,  
                telefone, 
                email,    
            }); err != nil {
                log.Printf("Erro ao escrever no arquivo de saída: %v", err)
            }
            outputCSV.Flush()
            fileMutex.Unlock()
        }

        time.Sleep(1 * time.Second)
	}
}

func consultarCNPJ(cnpj string) (*Empresa, error) {
	url := fmt.Sprintf("https://minhareceita.org/%s", cnpj)

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("erro na requisição HTTP: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code não OK: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler resposta: %v", err)
	}

	var empresa Empresa
	err = json.Unmarshal(body, &empresa)
	if err != nil {
		return nil, fmt.Errorf("erro ao decodificar JSON: %v", err)
	}

	return &empresa, nil
}

func validarCNPJ(cnpj string) bool {
	return len(cnpj) == 14
}