package router

import (
	"html/template"
	"io/fs"
	"strings"
	"testing"

	"github.com/v03413/bepusdt/static"
)

func TestLangGeCheckoutTemplateIsEmbedded(t *testing.T) {
	checkout, err := readCheckoutInfoFromFS(static.Checkout, "checkout/langge")
	if err != nil {
		t.Fatalf("read langge checkout info: %v", err)
	}

	if checkout.Name != "LangGe design" {
		t.Fatalf("unexpected checkout name: %q", checkout.Name)
	}
	if checkout.Author == "" {
		t.Fatal("checkout author is required")
	}
	if checkout.Desc == "" {
		t.Fatal("checkout desc is required")
	}

	view, err := fs.ReadFile(static.Checkout, "checkout/langge/views/checkout.html")
	if err != nil {
		t.Fatalf("read langge checkout template: %v", err)
	}
	if !strings.Contains(string(view), "{{ .trade_id }}") {
		t.Fatal("langge checkout template must only depend on trade_id server injection")
	}

	tmpl := template.New("default")
	if !registerTemplatesFromFS(tmpl, static.Checkout, "checkout/langge", "langge") {
		t.Fatal("langge checkout template was not registered")
	}
	if tmpl.Lookup("langge/checkout.html") == nil {
		t.Fatal("langge checkout template was not registered under expected name")
	}
}

func TestOfficialCheckoutFeeNoticeWraps(t *testing.T) {
	css, err := fs.ReadFile(static.Checkout, "checkout/official/assets/css/checkout.css")
	if err != nil {
		t.Fatalf("read official checkout CSS: %v", err)
	}

	content := string(css)
	if !strings.Contains(content, "white-space: normal;") || !strings.Contains(content, "overflow-wrap: anywhere;") {
		t.Fatal("official checkout fee notice must allow long localized text to wrap")
	}
	if strings.Contains(content, "text-overflow: ellipsis;") {
		t.Fatal("official checkout fee notice must not truncate localized text")
	}
}

func TestOfficialCheckoutPreservesLocale(t *testing.T) {
	js, err := fs.ReadFile(static.Checkout, "checkout/official/assets/js/checkout.js")
	if err != nil {
		t.Fatalf("read official checkout JavaScript: %v", err)
	}

	content := string(js)
	if !strings.Contains(content, "current.searchParams.set('lang', lang)") {
		t.Fatal("official checkout must canonicalize a missing locale in the browser URL")
	}
	if !strings.Contains(content, "locale: lang") {
		t.Fatal("official checkout must preserve locale when reselecting currency or network")
	}
}
