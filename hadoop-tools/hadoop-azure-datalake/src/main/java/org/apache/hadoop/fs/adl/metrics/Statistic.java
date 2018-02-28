package org.apache.hadoop.fs.adl.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistic which are collected in ADLS.
 */
public enum Statistic {

    OPEN                        ("OPEN", "OPEN", "counts"),
    OPEN_LATENCY                ("OPENLATENCY", "OPEN", "counts"),

    GETFILESTATUS               ("GETFILESTATUS", "", ""),
    GETFILESTATUS_LATENCY       ("GETFILESTATUSLATENCY", "", ""),

    MSGETFILESTATUS             ("MSGETFILESTATUS", "", ""),
    MSGETFILESTATUS_LATENCY     ("MSGETFILESTATUSLATENCY", "", ""),

    LISTSTATUS                  ("LISTSTATUS", "", ""),
    LISTSTATUS_LATENCY          ("LISTSTATUSLATENCY", "", ""),

    MSLISTSTATUS                ("MSLISTSTATUS", "", ""),
    MSLISTSTATUS_LATENCY        ("MSLISTSTATUSLATENCY", "", ""),

    GETCONTENTSUMMARY           ("GETCONTENTSUMMARY", "", ""),
    GETCONTENTSUMMARY_LATENCY   ("GETCONTENTSUMMARY", "", ""),

    GETFILECHECKSUM             ("GETFILECHECKSUM", "", ""),
    GETFILECHECKSUM_LATENCY     ("GETFILECHECKSUM", "", ""),

    GETACLSTATUS                ("GETACLSTATUS", "", ""),
    GETACLSTATUS_LATENCY        ("GETACLSTATUS", "", ""),

    MSGETACLSTATUS              ("MSGETACLSTATUS", "", ""),
    MSGETACLSTATUS_LATENCY      ("MSGETACLSTATUS", "", ""),

    CHECKACCESS                 ("CHECKACCESS", "", ""),
    CHECKACCESS_LATENCY         ("CHECKACCESS", "", ""),

    CREATE                      ("CREATE",             "", ""),
    CREATE_LATENCY              ("CREATE",             "", ""),

    MKDIRS                      ("MKDIRS",             "", ""),
    MKDIRS_LATENCY              ("MKDIRS",             "", ""),

    RENAME                      ("RENAME",             "", ""),
    RENAME_LATENCY              ("RENAME",             "", ""),

    SETOWNER                    ("SETOWNER",           "", ""),
    SETOWNER_LATENCY            ("SETOWNER",           "", ""),

    SETPERMISSION               ("SETPERMISSION",      "", ""),
    SETPERMISSION_LATENCY       ("SETPERMISSION",      "", ""),

    SETTIMES                    ("SETTIMES",           "", ""),
    SETTIMES_LATENCY            ("SETTIMES",           "", ""),

    MODIFYACLENTRIES            ("MODIFYACLENTRIES",   "", ""),
    MODIFYACLENTRIES_LATENCY    ("MODIFYACLENTRIES",   "", ""),

    REMOVEACLENTRIES            ("REMOVEACLENTRIES",   "", ""),
    REMOVEACLENTRIES_LATENCY    ("REMOVEACLENTRIES",   "", ""),

    REMOVEDEFAULTACL            ("REMOVEDEFAULTACL",   "", ""),
    REMOVEDEFAULTACL_LATENCY    ("REMOVEDEFAULTACL",   "", ""),

    REMOVEACL                   ("REMOVEACL",          "", ""),
    REMOVEACL_LATENCY           ("REMOVEACL",          "", ""),

    SETACL                      ("SETACL",             "SETACL", ""),
    SETACL_LATENCY              ("SETACL",             "SETACL", ""),

    CREATENONRECURSIVE          ("CREATENONRECURSIVE", "CREATENONRECURSIVE", ""),
    CREATENONRECURSIVE_LATENCY  ("CREATENONRECURSIVE", "CREATENONRECURSIVE", ""),

    APPEND                      ("APPEND", "", ""),
    APPEND_LATENCY              ("APPEND", "", ""),

    CONCAT                      ("CONCAT", "", ""),
    CONCAT_LATENCY              ("CONCAT", "", ""),

    MSCONCAT                    ("MSCONCAT", "", ""),
    MSCONCAT_LATENCY            ("MSCONCAT", "", ""),

    DELETE                      ("DELETE", "", ""),
    DELETE_LATENCY              ("DELETE", "", ""),

    CONCURRENTAPPEND            ("CONCURRENTAPPEND", "", ""),
    CONCURRENTAPPEND_LATENCY    ("CONCURRENTAPPEND", "", ""),

    SETEXPIRY                   ("SETEXPIRY", "", ""),
    SETEXPIRY_LATENCY           ("SETEXPIRY", "", ""),

    GETFILEINFO                 ("GETFILEINFO", "", ""),
    GETFILEINFO_LATENCY         ("GETFILEINFO", "", ""),


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