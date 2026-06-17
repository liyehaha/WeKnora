package doc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Tencent/WeKnora/cli/internal/cmdutil"
	"github.com/Tencent/WeKnora/cli/internal/iostreams"
	"github.com/Tencent/WeKnora/cli/internal/output"
	sdk "github.com/Tencent/WeKnora/client"
)

var docImportChunksFields = []string{
	"id", "knowledge_base_id", "title", "file_name", "file_type",
	"parse_status", "enable_status", "chunks_imported",
}

type ImportChunksOptions struct {
	DocumentsPath string
	ChunksPath    string
	TagID         string
	Channel       string
}

type ImportChunksService interface {
	ImportPreprocessedKnowledge(
		ctx context.Context,
		kbID string,
		request *sdk.PreprocessedKnowledgeImportRequest,
	) (*sdk.Knowledge, int, error)
}

type importedChunksResult struct {
	ID              string         `json:"id"`
	KnowledgeBaseID string         `json:"knowledge_base_id"`
	Title           string         `json:"title"`
	FileName        string         `json:"file_name"`
	FileType        string         `json:"file_type"`
	ParseStatus     string         `json:"parse_status"`
	EnableStatus    string         `json:"enable_status"`
	ChunksImported  int            `json:"chunks_imported"`
	Knowledge       *sdk.Knowledge `json:"knowledge,omitempty"`
}

func NewCmdImportChunks(f *cmdutil.Factory) *cobra.Command {
	opts := &ImportChunksOptions{}
	cmd := &cobra.Command{
		Use:   "import-chunks",
		Short: "Import externally preprocessed chunks into a knowledge base",
		Long: `Imports JSONL chunks produced by an external preprocessing pipeline.
The server preserves caller-provided chunk boundaries and metadata, then writes
WeKnora chunk rows and retrieval indices without running DocReader or the
built-in chunker again.`,
		Example: `  weknora doc import-chunks --documents preprocess/output/documents.jsonl --chunks preprocess/output/chunks.jsonl --kb my-kb
  weknora doc import-chunks --documents documents.jsonl --chunks chunks.jsonl --format json`,
		Args: cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			fopts, err := cmdutil.CheckFormatFlag(c)
			if err != nil {
				return err
			}
			fopts.ResolveDefault(iostreams.IO.IsStdoutTTY())
			if err := validateImportChunksOptions(opts); err != nil {
				return err
			}
			kbID, err := f.ResolveKB(c)
			if err != nil {
				return err
			}
			cli, err := f.Client()
			if err != nil {
				return err
			}
			return runImportChunks(c.Context(), opts, fopts, cli, kbID)
		},
	}
	cmdutil.AddKBFlag(cmd)
	cmd.Flags().StringVar(&opts.DocumentsPath, "documents", "", "Path to documents.jsonl from the preprocessing output")
	cmd.Flags().StringVar(&opts.ChunksPath, "chunks", "", "Path to chunks.jsonl from the preprocessing output")
	cmd.Flags().StringVar(&opts.TagID, "tag-id", "", "Tag id to associate with imported documents")
	cmd.Flags().StringVar(&opts.Channel, "channel", "", "Ingestion-channel tag recorded server-side (default \"api\")")
	_ = cmd.MarkFlagRequired("chunks")
	cmdutil.AddFormatFlag(cmd, docImportChunksFields...)
	cmdutil.SetAgentHelp(cmd, cmdutil.AgentHelp{
		UsedFor:       "Import external preprocessed chunks into the resolved knowledge base without server-side re-chunking.",
		RequiredFlags: []string{"--chunks"},
		Output:        "envelope.data is an array of imported documents with id, title, parse_status, chunks_imported",
	})
	return cmd
}

func validateImportChunksOptions(opts *ImportChunksOptions) error {
	if opts.ChunksPath == "" {
		return cmdutil.NewFlagError(fmt.Errorf("--chunks is required"))
	}
	if err := validateUploadPath(opts.ChunksPath); err != nil {
		return err
	}
	if opts.DocumentsPath != "" {
		if err := validateUploadPath(opts.DocumentsPath); err != nil {
			return err
		}
	}
	return nil
}

func runImportChunks(
	ctx context.Context,
	opts *ImportChunksOptions,
	fopts *cmdutil.FormatOptions,
	svc ImportChunksService,
	kbID string,
) error {
	documents, err := loadPreprocessedDocuments(opts.DocumentsPath)
	if err != nil {
		return err
	}
	chunks, err := loadPreprocessedChunks(opts.ChunksPath)
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		return cmdutil.NewError(cmdutil.CodeInputInvalidArgument, "chunks file contains no chunks")
	}

	requests := buildPreprocessedImportRequests(documents, chunks, opts)
	results := make([]importedChunksResult, 0, len(requests))
	for _, req := range requests {
		knowledge, count, err := svc.ImportPreprocessedKnowledge(ctx, kbID, &req)
		if err != nil {
			return cmdutil.WrapHTTP(err, "import preprocessed chunks for %s", req.Document.DocID)
		}
		results = append(results, importedChunksResult{
			ID:              knowledge.ID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID,
			Title:           knowledge.Title,
			FileName:        knowledge.FileName,
			FileType:        knowledge.FileType,
			ParseStatus:     knowledge.ParseStatus,
			EnableStatus:    knowledge.EnableStatus,
			ChunksImported:  count,
			Knowledge:       knowledge,
		})
	}

	if fopts.WantsJSON() {
		return fopts.Emit(iostreams.IO.Out, results, &output.Meta{Count: len(results)})
	}
	for _, result := range results {
		fmt.Fprintf(iostreams.IO.Out, "Imported %q (id: %s, chunks: %d)\n",
			result.Title, result.ID, result.ChunksImported)
	}
	return nil
}

func loadPreprocessedDocuments(path string) ([]sdk.PreprocessedKnowledgeDocument, error) {
	if path == "" {
		return nil, nil
	}
	return readJSONL[sdk.PreprocessedKnowledgeDocument](path)
}

func loadPreprocessedChunks(path string) ([]sdk.PreprocessedChunk, error) {
	return readJSONL[sdk.PreprocessedChunk](path)
}

func readJSONL[T any](path string) ([]T, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, cmdutil.Wrapf(cmdutil.CodeLocalFileIO, err, "open %s", path)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	var rows []T
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row T
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, cmdutil.Wrapf(cmdutil.CodeInputInvalidArgument, err,
				"parse %s line %d", path, lineNumber)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, cmdutil.Wrapf(cmdutil.CodeLocalFileIO, err, "read %s", path)
	}
	return rows, nil
}

func buildPreprocessedImportRequests(
	documents []sdk.PreprocessedKnowledgeDocument,
	chunks []sdk.PreprocessedChunk,
	opts *ImportChunksOptions,
) []sdk.PreprocessedKnowledgeImportRequest {
	docsByID := make(map[string]sdk.PreprocessedKnowledgeDocument, len(documents))
	order := make([]string, 0, len(documents))
	orderSeen := make(map[string]bool, len(documents))
	for _, document := range documents {
		docID := importDocumentID(document)
		if docID == "" {
			continue
		}
		document.DocID = docID
		if document.Title == "" {
			document.Title = document.DocTitle
		}
		if !orderSeen[docID] {
			order = append(order, docID)
			orderSeen[docID] = true
		}
		docsByID[docID] = document
	}

	chunksByDocID := make(map[string][]sdk.PreprocessedChunk)
	for _, chunk := range chunks {
		docID := strings.TrimSpace(chunk.DocID)
		if docID == "" {
			docID = strings.TrimSpace(chunk.SourcePath)
		}
		if docID == "" {
			docID = strings.TrimSpace(chunk.SourceFile)
		}
		if docID == "" {
			docID = "preprocessed"
		}
		chunk.DocID = docID
		chunksByDocID[docID] = append(chunksByDocID[docID], chunk)
		if _, exists := docsByID[docID]; !exists {
			docsByID[docID] = synthesizeDocumentFromChunk(chunk)
		}
		if !orderSeen[docID] {
			order = append(order, docID)
			orderSeen[docID] = true
		}
	}

	requests := make([]sdk.PreprocessedKnowledgeImportRequest, 0, len(order))
	for _, docID := range order {
		group := chunksByDocID[docID]
		if len(group) == 0 {
			continue
		}
		document := docsByID[docID]
		requests = append(requests, sdk.PreprocessedKnowledgeImportRequest{
			Document: document,
			Chunks:   group,
			Title:    document.Title,
			FileName: firstCLIImportNonEmpty(document.SourceFile, document.Title, document.DocID),
			TagID:    opts.TagID,
			Channel:  firstCLIImportNonEmpty(opts.Channel, uploadChannel),
		})
	}
	return requests
}

func importDocumentID(document sdk.PreprocessedKnowledgeDocument) string {
	return firstCLIImportNonEmpty(document.DocID, document.SourcePath, document.SourceFile)
}

func synthesizeDocumentFromChunk(chunk sdk.PreprocessedChunk) sdk.PreprocessedKnowledgeDocument {
	return sdk.PreprocessedKnowledgeDocument{
		DocID:      chunk.DocID,
		SourceFile: chunk.SourceFile,
		SourcePath: chunk.SourcePath,
		Title:      firstCLIImportNonEmpty(chunk.SourceFile, chunk.Title, chunk.HeadingPath, chunk.DocID),
		DocType:    chunk.DocType,
		Product:    chunk.Product,
		Language:   chunk.Language,
		Status:     chunk.Status,
	}
}

func firstCLIImportNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
