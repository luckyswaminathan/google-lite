package cis5550.jobs;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class URLBlacklist {
    // Pre-compiled patterns for better performance
    private static final Set<Pattern> COMPILED_PATTERNS;

    static {
        Set<String> rawPatterns = new HashSet<>(Set.of(
                // File extensions to block
                ".*\\.(png|jpe?g|gif|bmp|tiff|webp|ico|svg)$",
                ".*\\.(pdf|doc|docx|ppt|pptx|xls|xlsx|odt|ods|odp)$",
                ".*\\.(zip|rar|7z|tar|gz|bz2)$",
                ".*\\.(mp4|webm|avi|mov|wmv|flv|mkv)$",
                ".*\\.(mp3|wav|ogg|m4a|aac)$",
                ".*\\.(txt|csv|json|xml)$",
                ".*\\.(js|css|less|scss|map)$",
                ".*\\.(exe|dll|so|dmg|app|apk|deb|rpm)$",
                ".*\\.(php|asp|aspx|jsp|cgi)$",

                // Common administrative and utility paths
                ".*/archive/.*",
                ".*/tags/.*",
                ".*/search/.*",
                ".*/page/\\d+/.*",
                ".*/login/.*",
                ".*/signup/.*",
                ".*/signin/.*",
                ".*/signout/.*",
                ".*/logout/.*",
                ".*/register/.*",
                ".*/password/.*",
                ".*/reset-password/.*",
                ".*/forgot-password/.*",
                ".*/change-password/.*",
                ".*/authentication/.*",
                ".*/auth/.*",
                ".*/oauth/.*",
                ".*/oauth2/.*",
                ".*/sso/.*",
                ".*/login\\.(?:php|aspx|jsp|html).*",
                ".*/admin/.*",
                ".*/moderator/.*",
                ".*/dashboard/.*",
                ".*/cp/.*",             // Control panel
                ".*/cpanel/.*",
                ".*/webmaster/.*",
                ".*/rss/.*",
                ".*/feed/.*",
                ".*/atom/.*",
                ".*/sitemap/.*",
                ".*/sitemap\\.xml.*",
                ".*/robots\\.txt.*",
                ".*/api/.*",
                ".*/cdn-cgi/.*",
                ".*/wp-json/.*",
                ".*/wp-content/.*",
                ".*/wp-includes/.*",
                ".*/downloads/.*",
                ".*/download/.*",
                ".*/media/.*",
                ".*/static/.*",
                ".*/assets/.*",
                ".*/uploads/.*",
                ".*/themes/.*",
                ".*/plugins/.*",
                ".*/images/.*",
                ".*/img/.*",
                ".*/css/.*",
                ".*/js/.*",
                ".*/javascript/.*",
                ".*/fonts/.*",
                ".*/styles/.*",
                ".*/scripts/.*",
                ".*/temp/.*",
                ".*/tmp/.*",
                ".*/cache/.*",
                ".*/login.php.*",
                ".*/login.aspx.*",
                ".*/login.jsp.*",
                ".*/login.html.*",

                ".*/sitemap.xml.*",
                ".*/robots.txt.*",


                // Language-specific paths (ISO 639-1 codes + common variants)
                ".*/ab/.*",     // Abkhazian
                ".*/aa/.*",     // Afar
                ".*/af/.*",     // Afrikaans
                ".*/ak/.*",     // Akan
                ".*/sq/.*",     // Albanian
                ".*/am/.*",     // Amharic
                ".*/ar/.*",     // Arabic
                ".*/an/.*",     // Aragonese
                ".*/hy/.*",     // Armenian
                ".*/as/.*",     // Assamese
                ".*/av/.*",     // Avaric
                ".*/ae/.*",     // Avestan
                ".*/ay/.*",     // Aymara
                ".*/az/.*",     // Azerbaijani
                ".*/bm/.*",     // Bambara
                ".*/ba/.*",     // Bashkir
                ".*/eu/.*",     // Basque
                ".*/be/.*",     // Belarusian
                ".*/bn/.*",     // Bengali
                ".*/bi/.*",     // Bislama
                ".*/bs/.*",     // Bosnian
                ".*/br/.*",     // Breton
                ".*/bg/.*",     // Bulgarian
                ".*/my/.*",     // Burmese
                ".*/ca/.*",     // Catalan
                ".*/ch/.*",     // Chamorro
                ".*/ce/.*",     // Chechen
                ".*/ny/.*",     // Chichewa
                ".*/zh/.*",     // Chinese
                ".*/zh-cn/.*",  // Chinese (Simplified)
                ".*/zh-tw/.*",  // Chinese (Traditional)
                ".*/cv/.*",     // Chuvash
                ".*/kw/.*",     // Cornish
                ".*/co/.*",     // Corsican
                ".*/cr/.*",     // Cree
                ".*/hr/.*",     // Croatian
                ".*/cs/.*",     // Czech
                ".*/da/.*",     // Danish
                ".*/dv/.*",     // Divehi
                ".*/nl/.*",     // Dutch
                ".*/dz/.*",     // Dzongkha
                ".*/eo/.*",     // Esperanto
                ".*/et/.*",     // Estonian
                ".*/ee/.*",     // Ewe
                ".*/fo/.*",     // Faroese
                ".*/fj/.*",     // Fijian
                ".*/fi/.*",     // Finnish
                ".*/fr/.*",     // French
                ".*/ff/.*",     // Fulah
                ".*/gl/.*",     // Galician
                ".*/ka/.*",     // Georgian
                ".*/de/.*",     // German
                ".*/el/.*",     // Greek
                ".*/gn/.*",     // Guarani
                ".*/gu/.*",     // Gujarati
                ".*/ht/.*",     // Haitian
                ".*/ha/.*",     // Hausa
                ".*/he/.*",     // Hebrew
                ".*/hz/.*",     // Herero
                ".*/hi/.*",     // Hindi
                ".*/ho/.*",     // Hiri Motu
                ".*/hu/.*",     // Hungarian
                ".*/ia/.*",     // Interlingua
                ".*/id/.*",     // Indonesian
                ".*/ie/.*",     // Interlingue
                ".*/ga/.*",     // Irish
                ".*/ig/.*",     // Igbo
                ".*/ik/.*",     // Inupiaq
                ".*/io/.*",     // Ido
                ".*/is/.*",     // Icelandic
                ".*/it/.*",     // Italian
                ".*/iu/.*",     // Inuktitut
                ".*/ja/.*",     // Japanese
                ".*/jv/.*",     // Javanese
                ".*/kl/.*",     // Kalaallisut
                ".*/kn/.*",     // Kannada
                ".*/kr/.*",     // Kanuri
                ".*/ks/.*",     // Kashmiri
                ".*/kk/.*",     // Kazakh
                ".*/km/.*",     // Khmer
                ".*/ki/.*",     // Kikuyu
                ".*/rw/.*",     // Kinyarwanda
                ".*/ky/.*",     // Kyrgyz
                ".*/kv/.*",     // Komi
                ".*/kg/.*",     // Kongo
                ".*/ko/.*",     // Korean
                ".*/ku/.*",     // Kurdish
                ".*/kj/.*",     // Kuanyama
                ".*/la/.*",     // Latin
                ".*/lb/.*",     // Luxembourgish
                ".*/lg/.*",     // Ganda
                ".*/li/.*",     // Limburgan
                ".*/ln/.*",     // Lingala
                ".*/lo/.*",     // Lao
                ".*/lt/.*",     // Lithuanian
                ".*/lu/.*",     // Luba-Katanga
                ".*/lv/.*",     // Latvian
                ".*/gv/.*",     // Manx
                ".*/mk/.*",     // Macedonian
                ".*/mg/.*",     // Malagasy
                ".*/ms/.*",     // Malay
                ".*/ml/.*",     // Malayalam
                ".*/mt/.*",     // Maltese
                ".*/mi/.*",     // Maori
                ".*/mr/.*",     // Marathi
                ".*/mh/.*",     // Marshallese
                ".*/mn/.*",     // Mongolian
                ".*/na/.*",     // Nauru
                ".*/nv/.*",     // Navajo
                ".*/nd/.*",     // North Ndebele
                ".*/ne/.*",     // Nepali
                ".*/ng/.*",     // Ndonga
                ".*/nb/.*",     // Norwegian Bokmål
                ".*/nn/.*",     // Norwegian Nynorsk
                ".*/no/.*",     // Norwegian
                ".*/ii/.*",     // Sichuan Yi
                ".*/nr/.*",     // South Ndebele
                ".*/oc/.*",     // Occitan
                ".*/oj/.*",     // Ojibwa
                ".*/cu/.*",     // Church Slavic
                ".*/om/.*",     // Oromo
                ".*/or/.*",     // Oriya
                ".*/os/.*",     // Ossetian
                ".*/pa/.*",     // Panjabi
                ".*/pi/.*",     // Pali
                ".*/fa/.*",     // Persian
                ".*/pl/.*",     // Polish
                ".*/ps/.*",     // Pashto
                ".*/pt/.*",     // Portuguese
                ".*/qu/.*",     // Quechua
                ".*/rm/.*",     // Romansh
                ".*/rn/.*",     // Rundi
                ".*/ro/.*",     // Romanian
                ".*/ru/.*",     // Russian
                ".*/sa/.*",     // Sanskrit
                ".*/sc/.*",     // Sardinian
                ".*/sd/.*",     // Sindhi
                ".*/se/.*",     // Northern Sami
                ".*/sm/.*",     // Samoan
                ".*/sg/.*",     // Sango
                ".*/sr/.*",     // Serbian
                ".*/gd/.*",     // Scottish Gaelic
                ".*/sn/.*",     // Shona
                ".*/si/.*",     // Sinhala
                ".*/sk/.*",     // Slovak
                ".*/sl/.*",     // Slovenian
                ".*/so/.*",     // Somali
                ".*/st/.*",     // Southern Sotho
                ".*/es/.*",     // Spanish
                ".*/su/.*",     // Sundanese
                ".*/sw/.*",     // Swahili
                ".*/ss/.*",     // Swati
                ".*/sv/.*",     // Swedish
                ".*/ta/.*",     // Tamil
                ".*/te/.*",     // Telugu
                ".*/tg/.*",     // Tajik
                ".*/th/.*",     // Thai
                ".*/ti/.*",     // Tigrinya
                ".*/bo/.*",     // Tibetan
                ".*/tk/.*",     // Turkmen
                ".*/tl/.*",     // Tagalog
                ".*/tn/.*",     // Tswana
                ".*/to/.*",     // Tonga
                ".*/tr/.*",     // Turkish
                ".*/ts/.*",     // Tsonga
                ".*/tt/.*",     // Tatar
                ".*/tw/.*",     // Twi
                ".*/ty/.*",     // Tahitian
                ".*/ug/.*",     // Uighur
                ".*/uk/.*",     // Ukrainian
                ".*/ur/.*",     // Urdu
                ".*/uz/.*",     // Uzbek
                ".*/ve/.*",     // Venda
                ".*/vi/.*",     // Vietnamese
                ".*/vo/.*",     // Volapük
                ".*/wa/.*",     // Walloon
                ".*/cy/.*",     // Welsh
                ".*/wo/.*",     // Wolof
                ".*/fy/.*",     // Western Frisian
                ".*/xh/.*",     // Xhosa
                ".*/yi/.*",     // Yiddish
                ".*/yo/.*",     // Yoruba
                ".*/za/.*",     // Zhuang
                ".*/zu/.*",     // Zulu

                // Wikipedia language domains
                "^https?://[a-z]{2,3}\\.wikipedia\\.org/.*",  // Catches all non-English Wikipedia domains
                "^https?://[a-z]{2,3}\\.m\\.wikipedia\\.org/.*",  // Mobile Wikipedia domains

                // Forums and user paths in different languages
                ".*/forum/[^/]+.*",
                ".*/forums/[^/]+.*",
                ".*/f/[^/]+.*",
                ".*/community/[^/]+.*",
                ".*/comunidad/.*",      // Spanish community
                ".*/communaute/.*",     // French community
                ".*/gemeinschaft/.*",   // German community
                ".*/comunita/.*",       // Italian community

                // User-related paths in different languages
                ".*/members/.*",
                ".*/member/.*",
                ".*/profile/.*",
                ".*/profiles/.*",
                ".*/user/.*",
                ".*/users/.*",
                ".*/usuario/.*",        // Spanish
                ".*/utilisateur/.*",    // French
                ".*/benutzer/.*",       // German
                ".*/utente/.*",         // Italian
                ".*/gebruiker/.*",      // Dutch
                ".*/kullanici/.*",      // Turkish
                ".*/brukare/.*",        // Swedish
                ".*/bruger/.*",         // Danish
                ".*/bruker/.*",         // Norwegian
                ".*/kayttaja/.*",       // Finnish
                ".*/uzytkownik/.*",     // Polish
                ".*/felhasznalo/.*",    // Hungarian
                ".*/korisnik/.*",       // Croatian
                ".*/potret/.*",         // Romanian
                ".*/zhuce/.*",          // Chinese
                ".*/profil/.*",
                ".*/conta/.*",          // Portuguese
                ".*/hesap/.*",          // Turkish
                ".*/konto/.*",          // German/Polish
                ".*/compte/.*",         // French
                ".*/cuenta/.*",         // Spanish
                ".*/conto/.*",          // Italian
                ".*/akun/.*",           // Indonesian
                ".*/perfil/.*",         // Spanish/Portuguese profile

                // Common content management system paths
                ".*/wp-login/.*",
                ".*/wp-admin/.*",
                ".*/administrator/.*",
                ".*/admincp/.*",
                ".*/adminpanel/.*",
                ".*/webadmin/.*",
                ".*/admindashboard/.*",
                ".*/admin-console/.*",
                ".*/controlpanel/.*",
                ".*/modcp/.*",          // Moderator control panel
                ".*/usercp/.*",         // User control panel
                ".*/panel/.*"
        ));

        // Pre-compile all patterns
        COMPILED_PATTERNS = rawPatterns.stream()
                .map(pattern -> Pattern.compile(pattern, Pattern.CASE_INSENSITIVE))
                .collect(Collectors.toSet());
    }

    /**
     * Checks if a URL matches any blacklisted pattern
     * @param url The URL to check
     * @return true if the URL should be blacklisted, false otherwise
     */
    public static boolean isBlacklisted(String url) {
        if (url == null || url.isEmpty()) {
            return true;
        }

        // Normalize URL by converting to lowercase and removing trailing slashes
        String normalizedUrl = url.toLowerCase().replaceAll("/+$", "");

        // Check against all compiled patterns
        return COMPILED_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(normalizedUrl).matches());
    }

    /**
     * Helper method to check multiple URLs at once
     * @param urls Collection of URLs to check
     * @return Set of URLs that are blacklisted
     */
    public static Set<String> getBlacklistedUrls(Set<String> urls) {
        return urls.stream()
                .filter(URLBlacklist::isBlacklisted)
                .collect(Collectors.toSet());
    }
}