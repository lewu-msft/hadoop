package org.apache.hadoop.fs.adl.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistic which are collected in ADLS.
 */
public enum Statistic {

    OPEN                        ("OPEN", "OPEN", "counts"),
    OPEN_LATENCY                ("OPENLATENCY", "OPENLATENCY", "counts"),

    GETFILESTATUS               ("GETFILESTATUS", "GETFILESTATUS", "counts"),
    GETFILESTATUS_LATENCY       ("GETFILESTATUSLATENCY", "GETFILESTATUSLATENCY", "counts"),

    MSGETFILESTATUS             ("MSGETFILESTATUS", "MSGETFILESTATUS", "counts"),
    MSGETFILESTATUS_LATENCY     ("MSGETFILESTATUSLATENCY", "MSGETFILESTATUSLATENCY", "counts"),

    LISTSTATUS                  ("LISTSTATUS", "LISTSTATUS", "counts"),
    LISTSTATUS_LATENCY          ("LISTSTATUSLATENCY", "LISTSTATUSLATENCY", "counts"),

    MSLISTSTATUS                ("MSLISTSTATUS", "MSLISTSTATUS", "counts"),
    MSLISTSTATUS_LATENCY        ("MSLISTSTATUSLATENCY", "MSLISTSTATUSLATENCY", "counts"),

    GETCONTENTSUMMARY           ("GETCONTENTSUMMARY", "GETCONTENTSUMMARY", "counts"),
    GETCONTENTSUMMARY_LATENCY   ("GETCONTENTSUMMARYLATENCY", "GETCONTENTSUMMARYLATENCY", "counts"),

    GETFILECHECKSUM             ("GETFILECHECKSUM", "GETFILECHECKSUM", "counts"),
    GETFILECHECKSUM_LATENCY     ("GETFILECHECKSUMLATENCY", "GETFILECHECKSUMLATENCY", "counts"),

    GETACLSTATUS                ("GETACLSTATUS", "GETACLSTATUS", "counts"),
    GETACLSTATUS_LATENCY        ("GETACLSTATUSLATENCY", "GETACLSTATUSLATENCY", "counts"),

    MSGETACLSTATUS              ("MSGETACLSTATUS", "MSGETACLSTATUS", "counts"),
    MSGETACLSTATUS_LATENCY      ("MSGETACLSTATUSLATENCY", "MSGETACLSTATUSLATENCY", "counts"),

    CHECKACCESS                 ("CHECKACCESS", "CHECKACCESS", "counts"),
    CHECKACCESS_LATENCY         ("CHECKACCESSLATENCY", "CHECKACCESSLATENCY", "counts"),

    CREATE                      ("CREATE", "CREATE", "counts"),
    CREATE_LATENCY              ("CREATELATENCY", "CREATELATENCY", "counts"),

    MKDIRS                      ("MKDIRS", "MKDIRS", "counts"),
    MKDIRS_LATENCY              ("MKDIRSLATENCY", "MKDIRSLATENCY", "counts"),

    RENAME                      ("RENAME", "RENAME", "counts"),
    RENAME_LATENCY              ("RENAMELATENCY", "RENAMELATENCY", "counts"),

    SETOWNER                    ("SETOWNER",           "", ""),
    SETOWNER_LATENCY            ("SETOWNERLATENCY",           "", ""),

    SETPERMISSION               ("SETPERMISSION",      "", ""),
    SETPERMISSION_LATENCY       ("SETPERMISSIONLATENCY",      "", ""),

    SETTIMES                    ("SETTIMES",           "", ""),
    SETTIMES_LATENCY            ("SETTIMESLATENCY",           "", ""),

    MODIFYACLENTRIES            ("MODIFYACLENTRIES",   "", ""),
    MODIFYACLENTRIES_LATENCY    ("MODIFYACLENTRIESLATENCY",   "", ""),

    REMOVEACLENTRIES            ("REMOVEACLENTRIES",   "", ""),
    REMOVEACLENTRIES_LATENCY    ("REMOVEACLENTRIESLATENCY",   "", ""),

    REMOVEDEFAULTACL            ("REMOVEDEFAULTACL",   "", ""),
    REMOVEDEFAULTACL_LATENCY    ("REMOVEDEFAULTACLLATENCY",   "", ""),

    REMOVEACL                   ("REMOVEACL",          "", ""),
    REMOVEACL_LATENCY           ("REMOVEACLLATENCY",          "", ""),

    SETACL                      ("SETACL",             "SETACL", ""),
    SETACL_LATENCY              ("SETACLLATENCY",             "SETACL", ""),

    CREATENONRECURSIVE          ("CREATENONRECURSIVE", "CREATENONRECURSIVE", ""),
    CREATENONRECURSIVE_LATENCY  ("CREATENONRECURSIVELATENCY", "CREATENONRECURSIVE", ""),

    APPEND                      ("APPEND", "", ""),
    APPEND_LATENCY              ("APPENDLATENCY", "", ""),

    CONCAT                      ("CONCAT", "", ""),
    CONCAT_LATENCY              ("CONCATLATENCY", "", ""),

    MSCONCAT                    ("MSCONCAT", "", ""),
    MSCONCAT_LATENCY            ("MSCONCATLATENCY", "", ""),

    DELETE                      ("DELETE", "", ""),
    DELETE_LATENCY              ("DELETELATENCY", "", ""),

    CONCURRENTAPPEND            ("CONCURRENTAPPEND", "", ""),
    CONCURRENTAPPEND_LATENCY    ("CONCURRENTAPPENDLATENCY", "", ""),

    SETEXPIRY                   ("SETEXPIRY", "", ""),
    SETEXPIRY_LATENCY           ("SETEXPIRYLATENCY", "", ""),

    GETFILEINFO                 ("GETFILEINFO", "", ""),
    GETFILEINFO_LATENCY         ("GETFILEINFOLATENCY", "", ""),


    READ_CATEGORY               ("readCategory", "", ""),
    WRITE_CATEGORY              ("writeCategory", "", ""),
    METADATA_CATEGORY           ("metadataCategory", "", ""),
    ACL_CATEGORY                ("aclCategory", "", ""),
    FSOPS_CATEGORY              ("fsopsCategory", "", "");

    private static final Map<String, Statistic> SYMBOL_MAP =
            new HashMap<>(Statistic.values().length);

    static {
        for (Statistic stat : values()) {
            SYMBOL_MAP.put(stat.getSymbol(), stat);
        }
    }

    Statistic(String symbol, String description, String unit) {
        this.symbol = symbol;
        this.description = description;
        this.unit = unit;
    }

    private final String symbol;
    private final String description;
    private final String unit;

    public String getSymbol() {
        return symbol;
    }

    /**
     * Get a statistic from a symbol.
     * @param symbol statistic to look up
     * @return the value or null.
     */
    public static Statistic fromSymbol(String symbol) {
        return SYMBOL_MAP.get(symbol);
    }

    public String getDescription() {
        return description;
    }

    public String getUnit() {
        return unit;
    }

    /**
     * The string value is simply the symbol.
     * This makes this operation very low cost.
     * @return the symbol of this statistic.
     */
    @Override
    public String toString() {
        return symbol;
    }
}