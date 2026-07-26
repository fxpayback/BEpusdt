package model

import "testing"

func TestCheckoutURLWithLocale(t *testing.T) {
	tests := []struct {
		name, locale, want string
	}{
		{name: "Chinese locale", locale: "zh-HK", want: "https://pay.example/pay/checkout/trade-1?lang=zh"},
		{name: "English locale", locale: "en-US", want: "https://pay.example/pay/checkout/trade-1?lang=en"},
		{name: "unsupported locale", locale: "fr-FR", want: "https://pay.example/pay/checkout/trade-1"},
		{name: "empty locale", locale: "", want: "https://pay.example/pay/checkout/trade-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkoutURLWithLocale("https://pay.example/pay/checkout/trade-1", tt.locale); got != tt.want {
				t.Fatalf("checkoutURLWithLocale() = %q, want %q", got, tt.want)
			}
		})
	}
}
