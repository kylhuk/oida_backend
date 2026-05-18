package vesselfinder

import "strings"

// IsBotPage returns true if the HTML looks like a Cloudflare challenge or captcha page.
func IsBotPage(html string) bool {
	lower := strings.ToLower(html)
	return strings.Contains(lower, "checking if the site connection is secure") ||
		strings.Contains(lower, "verify you are human") ||
		strings.Contains(lower, "cf-challenge") ||
		strings.Contains(lower, "g-recaptcha") ||
		strings.Contains(lower, "h-captcha")
}
