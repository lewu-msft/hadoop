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

    SETOWNER                    ("SETOWNER", "SETOWNER", "counts"),
    SETOWNER_LATENCY            ("SETOWNERLATENCY", "SETOWNERLATENCY", "counts"),

    SETPERMISSION               ("SETPERMISSION", "SETPERMISSION", "counts"),
    SETPERMISSION_LATENCY       ("SETPERMISSIONLATENCY", "SETPERMISSIONLATENCY", "counts"),

    SETTIMES                    ("SETTIMES", "SETTIMES", "counts"),
    SETTIMES_LATENCY            ("SETTIMESLATENCY", "SETTIMESLATENCY", "counts"),

    MODIFYACLENTRIES            ("MODIFYACLENTRIES", "MODIFYACLENTRIES", "counts"),
    MODIFYACLENTRIES_LATENCY    ("MODIFYACLENTRIESLATENCY", "MODIFYACLENTRIESLATENCY", "counts"),

    REMOVEACLENTRIES            ("REMOVEACLENTRIES", "REMOVEACLENTRIES", "counts"),
    REMOVEACLENTRIES_LATENCY    ("REMOVEACLENTRIESLATENCY", "REMOVEACLENTRIESLATENCY", "counts"),

    REMOVEDEFAULTACL            ("REMOVEDEFAULTACL", "REMOVEDEFAULTACL", "counts"),
    REMOVEDEFAULTACL_LATENCY    ("REMOVEDEFAULTACLLATENCY", "REMOVEDEFAULTACLLATENCY", "counts"),

    REMOVEACL                   ("REMOVEACL", "REMOVEACL", "counts"),
    REMOVEACL_LATENCY           ("REMOVEACLLATENCY", "REMOVEACLLATENCY", "counts"),

    SETACL                      ("SETACL", "SETACL", "counts"),
    SETACL_LATENCY              ("SETACLLATENCY", "SETACLLATENCY", "counts"),

    CREATENONRECURSIVE          ("CREATENONRECURSIVE", "CREATENONRECURSIVE", "counts"),
    CREATENONRECURSIVE_LATENCY  ("CREATENONRECURSIVELATENCY", "CREATENONRECURSIVELATENCY", "counts"),

    APPEND                      ("APPEND", "APPEND", "counts"),
    APPEND_LATENCY              ("APPENDLATENCY", "APPENDLATENCY", "counts"),

    CONCAT                      ("CONCAT", "CONCAT", "counts"),
    CONCAT_LATENCY              ("CONCATLATENCY", "CONCATLATENCY", "counts"),

    MSCONCAT                    ("MSCONCAT", "MSCONCAT", "counts"),
    MSCONCAT_LATENCY            ("MSCONCATLATENCY", "MSCONCATLATENCY", "counts"),

    DELETE                      ("DELETE", "DELETE", "counts"),
    DELETE_LATENCY              ("DELETELATENCY", "DELETELATENCY", "counts"),

    CONCURRENTAPPEND            ("CONCURRENTAPPEND", "CONCURRENTAPPEND", "counts"),
    CONCURRENTAPPEND_LATENCY    ("CONCURRENTAPPENDLATENCY", "CONCURRENTAPPENDLATENCY", "counts"),

    SETEXPIRY                   ("SETEXPIRY", "SETEXPIRY", "counts"),
    SETEXPIRY_LATENCY           ("SETEXPIRYLATENCY", "SETEXPIRYLATENCY", "counts"),

    GETFILEINFO                 ("GETFILEINFO", "GETFILEINFO", "counts"),
    GETFILEINFO_LATENCY         ("GETFILEINFOLATENCY", "GETFILEINFOLATENCY", "counts"),


    READ_CATEGORY               ("READCATEGORY", "READCATEGORY", "counts"),
    WRITE_CATEGORY              ("WRITECATEGORY", "WRITECATEGORY", "counts"),
    METADATA_CATEGORY           ("METADATACATEGORY", "METADATACATEGORY", "counts"),
    ACL_CATEGORY                ("ACLCATEGORY", "ACLCATEGORY", "counts"),
    FSOPS_CATEGORY              ("FSOPSCATEGORY", "FSOPSCATEGORY", "counts");

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