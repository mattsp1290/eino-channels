package slack

import "github.com/mattsp1290/eino-channels/internal/render"

func conversationEscape(text string) string { return render.EscapeSlack(text) }

func chunkSlack(escaped string) []string {
	return render.Chunk(escaped, render.SlackChunkChars, render.SlackMeasure)
}

func previewSlack(escaped string) string {
	return render.Preview(escaped, render.SlackChunkChars, render.SlackMeasure)
}
