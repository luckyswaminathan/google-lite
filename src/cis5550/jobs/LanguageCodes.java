package cis5550.jobs;

import java.util.Optional;
import java.util.Set;

public class LanguageCodes {
    // ISO 639-1 two-letter language codes
    private static final Set<String> ISO_639_1_CODES = Set.of(
            // Major European languages
            "de", // German
            "es", // Spanish
            "fr", // French
            "it", // Italian
            "pt", // Portuguese
            "ru", // Russian
            "nl", // Dutch
            "pl", // Polish
            "tr", // Turkish

            // Asian languages
            "zh", // Chinese
            "ja", // Japanese
            "ko", // Korean
            "th", // Thai
            "vi", // Vietnamese
            "hi", // Hindi
            "bn", // Bengali
            "id", // Indonesian
            "ms", // Malay

            // Nordic languages
            "sv", // Swedish
            "da", // Danish
            "fi", // Finnish
            "no", // Norwegian
            "is", // Icelandic

            // Eastern European
            "cs", // Czech
            "sk", // Slovak
            "hu", // Hungarian
            "uk", // Ukrainian
            "el", // Greek
            "bg", // Bulgarian
            "hr", // Croatian
            "sr", // Serbian
            "sl", // Slovenian
            "ro", // Romanian

            // Baltic languages
            "et", // Estonian
            "lv", // Latvian
            "lt", // Lithuanian

            // Middle Eastern
            "ar", // Arabic
            "he", // Hebrew
            "fa", // Persian/Farsi
            "ur", // Urdu

            // South Asian
            "ta", // Tamil
            "te", // Telugu
            "ml", // Malayalam
            "kn", // Kannada
            "mr", // Marathi
            "gu", // Gujarati

            // Southeast Asian
            "tl", // Tagalog/Filipino
            "my", // Burmese
            "km", // Khmer

            // Others
            "ka", // Georgian
            "hy", // Armenian
            "az", // Azerbaijani
            "uz", // Uzbek
            "kk" // Kazakh
    );

    // Common country codes for language variants
    private static final Set<String> COUNTRY_CODES = Set.of(
            // Spanish variants
            "es-mx", "es-ar", "es-co", "es-cl", "es-pe", "es-ve", "es-ec",

            // French variants
            "fr-ca", "fr-be", "fr-ch", "fr-lu",

            // Portuguese variants
            "pt-br", "pt-ao", "pt-mz",

            // German variants
            "de-at", "de-ch", "de-lu",

            // Chinese variants
            "zh-cn", "zh-tw", "zh-hk", "zh-sg",

            // English variants (for reference)
            "en-gb", "en-au", "en-ca", "en-nz", "en-ie", "en-za", "en-in",

            // Arabic variants
            "ar-sa", "ar-ae", "ar-eg", "ar-kw", "ar-qa",

            // Other variants
            "fr-ma", // French (Morocco)
            "nl-be", // Dutch (Belgium)
            "sv-fi"  // Swedish (Finland)
    );

    // Common language subdomains
    private static final Set<String> LANGUAGE_SUBDOMAINS = Set.of(
            // Common formats
            "international", "global", "world",

            // Language-specific
            "arabic", "chinese", "spanish", "russian", "japanese",
            "korean", "turkish", "portuguese", "french", "german",
            "italian", "dutch", "polish", "swedish", "danish",

            // Regional
            "asia", "europe", "latinamerica", "middleeast",

            // Country-specific
            "mexico", "brasil", "india", "china", "japan",
            "russia", "france", "germany", "spain", "italy"
    );

    // Method to check if a URL contains non-English language indicators
    public static boolean containsNonEnglishLanguage(String url) {
        String lowerUrl = url.toLowerCase();

        // Check for language codes
        for (String code : ISO_639_1_CODES) {
            // Check common patterns
            if (lowerUrl.contains("/" + code + "/") ||      // Path segment
                    lowerUrl.startsWith(code + ".") ||          // Subdomain
                    lowerUrl.contains("lang=" + code) ||        // URL parameter
                    lowerUrl.contains("language=" + code) ||    // URL parameter
                    lowerUrl.contains("locale=" + code)) {      // URL parameter
                return true;
            }
        }

        // Check for country-specific variants
        for (String code : COUNTRY_CODES) {
            if (lowerUrl.contains("/" + code + "/") ||
                    lowerUrl.contains("lang=" + code) ||
                    lowerUrl.contains("language=" + code) ||
                    lowerUrl.contains("locale=" + code)) {
                return true;
            }
        }

        // Check for language subdomains
        for (String subdomain : LANGUAGE_SUBDOMAINS) {
            if (lowerUrl.startsWith(subdomain + ".")) {
                return true;
            }
        }

        return false;
    }

    // Method to check if a path segment indicates non-English content
    public static boolean isNonEnglishPath(String pathSegment) {
        String lowerSegment = pathSegment.toLowerCase();
        return ISO_639_1_CODES.contains(lowerSegment) ||
                COUNTRY_CODES.contains(lowerSegment) ||
                LANGUAGE_SUBDOMAINS.contains(lowerSegment);
    }

    // Method to get language code from URL if present
    public static Optional<String> extractLanguageCode(String url) {
        String lowerUrl = url.toLowerCase();

        // Check path segments
        String[] segments = lowerUrl.split("/");
        for (String segment : segments) {
            if (ISO_639_1_CODES.contains(segment)) {
                return Optional.of(segment);
            }
        }

        // Check URL parameters
        if (lowerUrl.contains("lang=") || lowerUrl.contains("language=") || lowerUrl.contains("locale=")) {
            for (String code : ISO_639_1_CODES) {
                if (lowerUrl.contains("lang=" + code) ||
                        lowerUrl.contains("language=" + code) ||
                        lowerUrl.contains("locale=" + code)) {
                    return Optional.of(code);
                }
            }
        }

        return Optional.empty();
    }
}