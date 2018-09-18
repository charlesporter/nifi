import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.processors.script.ExecuteScript

import java.nio.charset.StandardCharsets
import groovy.json.JsonOutput
import groovy.util.slurpersupport.GPathResult

import java.util.regex.Pattern


//for debugging
def line_no = "A.0"

interface W2k3AttributeParser {
    public String getValue(String raw_data);
}

class W2k3Const {
    static Pattern CTRL_CHARS   = Pattern.compile('\\p{Cntrl}')
    static final int FLOW_BATCH_SIZE = 100
    static final Pattern common_event_id_regex = Pattern.compile("(?<=<EventID>)(.*)(?=<\\/EventID>)")
    static final boolean DEBUG = false
}

class W2k3CoreParser {

    def log
    String type
    String tostring
    HashMap<String, Pattern> hmAttributes;

    W2k3CoreParser(GPathResult gpr, def _log) {
        log = _log
        type = gpr.@type.text()
        hmAttributes = new HashMap<>()

        if (W2k3Const.DEBUG) {
            log.warn("====================================================================================");
            log.warn("W2k3CoreParser.type = {}", [type] as Object[])
            log.warn("====================================================================================");
        }

        gpr.attr.each { child ->

            def name = child.@name.text()
            def pattern = child.@regexp.text()

            if (W2k3Const.DEBUG) {
                log.warn("====================================================================================");
                log.warn("W2k3CoreParser.attr.name = {}", [name] as Object[])
                log.warn("W2k3CoreParser.attr.pattern = {}", [pattern] as Object[])
                log.warn("====================================================================================");
            }
            hmAttributes.put(name, Pattern.compile(pattern))
        }
    }

    String getValue(String name, String log_body) {
        def s = ''
        Pattern p = hmAttributes.get(name)
        if (p) {
            s = log_body.find(p)

        } else {
            if (W2k3Const.DEBUG) {
                log.warn("====================================================================================");
                log.warn("NO CORE ATTRIBUTE FOUND THAT MATCHES " + name)
                log.warn(toString())
                log.warn("====================================================================================");
            }
        }
        return s
    }

    String toString() {
        if (tostring == "") {
            StringBuilder sb = new StringBuilder()
            sb.append("===============================================================\n")
            sb.append("CORE PROCESSOR TYPE ").append(type).append("\n")
            sb.append("---------------------------------------------------------------\n")
            hmAttributes.each { k, v ->
                sb.append(k).append(" : ").append(v.pattern()).append("\n")
            }
            sb.append("===============================================================\n")
            tostring = sb.toString()
        }
        return tostring
    }
}

class W2k3ExtParser {
    def loggger
    String type
    String eventID
    boolean blocked
    String regexp
    Pattern dataPattern
    String splittoken
    HashMap<String, W2k3AttributeParser> hmAttributeParsers

    Closure getAttributeValues = { String rawdata ->
        def smalldata = rawdata.find(dataPattern)
        Properties rval = new Properties()
        if (smalldata && hmAttributeParsers) {

            hmAttributeParsers.each { k, v ->
                if (!v.blocked) {
                    def val = v.getValue(smalldata)
                    if (val && val != "-" && val != "" && val != "0")
                        rval.put(v.name, v.getValue(smalldata))
                }
            }
        }
        return rval
    }

    W2k3ExtParser(GPathResult gpr, def _logger) {
        eventID = gpr.@event_id.text()
        type = gpr.@type.text()
        blocked = gpr.@blocked.toBoolean()
        regexp = gpr."ext-data".@regexp.text()
        dataPattern = Pattern.compile(regexp)
        splittoken = gpr."ext-data".@"split-token".text()

        loggger = _logger

        if (W2k3Const.DEBUG) {
            loggger.warn("====================================================================================");
            loggger.warn("W2k3ExtParser.event_id = {}", [eventID] as Object[])
            loggger.warn("W2k3ExtParser.blocked = {}", [blocked] as Object[])
            loggger.warn("W2k3ExtParser.type = {}", [type] as Object[])
            loggger.warn("W2k3ExtParser.ext_data.@regexp = {}", [regexp] as Object[])
            loggger.warn("W2k3ExtParser.ext_data.@split-token = {}", [splittoken] as Object[])
            loggger.warn("====================================================================================");
        }

        hmAttributeParsers = new HashMap<>()
        gpr."ext-data"."ext-attr".each { attr_mapping ->
            W2k3AttributeParser ap
            if (type == "legacy")
                ap = new LegacyAttributeParser(attr_mapping, splittoken, loggger)
            else
                ap = new StandardAttributeParser(attr_mapping, loggger)
            if (ap)
                hmAttributeParsers.put(ap.name, ap)
            else {
                if (W2k3Const.DEBUG) {
                    loggger.error("NOT ABLE TO CREATE ATTRIBUTE PARSER {} ", [attr_mapping.text()])
                }
            }
        }
    }

    class LegacyAttributeParser implements W2k3AttributeParser {
        String name
        boolean blocked
        int idx
        String splittoken
        def logger

        LegacyAttributeParser(GPathResult gpr, String _splittoken, def _logger) {
            logger = _logger
            splittoken = _splittoken
            name = gpr.@name.text()
            blocked = gpr.@blocked.toBoolean()
            idx = gpr.@index.toInteger()

            if (W2k3Const.DEBUG) {
                logger.warn("====================================================================================");
                logger.warn("LegacyAttributeParser.name = {}", [name] as Object[])
                logger.warn("LegacyAttributeParser.blocked = {}", [blocked] as Object[])
                logger.warn("LegacyAttributeParser.idx = {}", [idx] as Object[])
                logger.warn("LegacyAttributeParser.splittoken = {}", [splittoken] as Object[])
                logger.warn("====================================================================================");
            }
        }

        String getValue(String rawdata) {
            if (blocked) {
                return "-blocked-";
            }
            String[] parts = null
            if(rawdata) {
                parts = rawdata.split(splittoken)
            }
            if(parts && parts.length > idx) {
                if (W2k3Const.DEBUG) {
                    logger.warn("====================================================================================");
                    logger.warn("LegacyAttributeParser.parse({})", [rawdata] as Object[])
                    logger.warn("LegacyAttributeParser.idx = {}", [idx] as Object[])
                    logger.warn("LegacyAttributeParser.splittoken = {}", [splittoken] as Object[])
                    logger.warn("LegacyAttributeParser.parts[].length = {}", [parts.length] as Object[])
                    for (int x = 0; x < parts.length; x++) {
                        logger.warn("LegacyAttributeParser.parts[{}] = {}", [x, parts[x]] as Object[])
                    }
                    logger.warn("====================================================================================");
                }
                return parts[idx]
            }
            return '';
        }
    }

    class StandardAttributeParser implements W2k3AttributeParser {
        String name
        boolean blocked
        Pattern attrPattern
        String regexp
        def logger

        StandardAttributeParser(GPathResult gpr, def log) {
            logger = log
            name = gpr.@name.text()
            blocked = gpr.@blocked.toBoolean()
            regexp = gpr.@regexp.text()
            attrPattern = Pattern.compile(regexp)

            if (W2k3Const.DEBUG) {
                logger.warn("====================================================================================");
                logger.warn("StandardAttributeParser.name = {}", [name] as Object[])
                logger.warn("StandardAttributeParser.blocked = {}", [blocked] as Object[])
                logger.warn("StandardAttributeParser.regexp = {}", [regexp] as Object[])
                logger.warn("====================================================================================");
            }
        }

        String getValue(String data) {
            if (blocked) {
                return "-blocked-";
            }
            if(!data) {
              return null;
            }
            if (W2k3Const.DEBUG) {
                loggger.warn("====================================================================================");
                loggger.warn("StandardAttributeParser.parse({})", [data] as Object[])
                loggger.warn("StandardAttributeParser.regexp = {}", [regexp] as Object[])
                loggger.warn("====================================================================================");
            }
            return data.find(attrPattern)
        }
    }
}

class W2k3Parser {
    W2k3CoreParser coreParser
    W2k3ExtParser extParser
    String eventID
    def logger


    W2k3Parser(String _eventID, W2k3ExtParser _extParser, W2k3CoreParser _coreParser, def _logger) {
        eventID = _eventID
        logger = _logger
        extParser = _extParser
        coreParser = _coreParser
    }

    W2k3CoreData getCoreDataObj(String log_body) {


        W2k3CoreData cd = new W2k3CoreData()
        cd.with {


            event_key = UUID.randomUUID().toString() + "-" + coreParser.getValue("record-id", log_body)
            event_id = coreParser.getValue("event-id", log_body)
            event_time = coreParser.getValue("event-time", log_body)
            log_provider = coreParser.getValue("log-provider", log_body)
            host_name = coreParser.getValue("host-name", log_body)
            host_ip = W2k3HostIPAddresses.getIP(host_name)
            host_domain = 'n/a'
            if (host_name && host_name.contains('.')) {
                host_domain = host_name.substring(host_name.indexOf('.') + 1)
                host_name = host_name.substring(0, host_name.indexOf('.'))
            }
            event_date = event_time.substring(0, 10)
            log_type = "windows-security-log"

        }
        return cd
    }


    ArrayList<W2k3ExtData> getExtDataObjs(String log_body, W2k3CoreData cd) {

        Properties p = extParser.getAttributeValues(log_body)
        ArrayList<W2k3ExtData> alExtDataObjs = new ArrayList<>()

        p.each { k, v ->

            W2k3ExtData objExtData = new W2k3ExtData()
            objExtData.with {
                //just a copy of these three elements
                event_key = cd.event_key
                event_id = cd.event_id
                event_date = cd.event_date
                attr_name = k
                value = v

            }

            alExtDataObjs.add(objExtData)
        }
        return alExtDataObjs
    }
    Closure blocked = { extParser.blocked }
}


class W2k3Parsers {

    static final String TEMPLATE_PATH = "/usr/nifi/misc-conf/event_mapping.xml"
    static Map<String, W2k3Parser> hmParsers;

    synchronized static W2k3Parser getParser(String eventID, def logger) {
        if (hmParsers == null) {

            hmParsers = new HashMap<>();

            XmlSlurper xmlSlurper = new XmlSlurper()
            GPathResult template = xmlSlurper.parseText(new File(TEMPLATE_PATH).getText())

//            int core_mapping_count = template."core-mapping".size()
//            int event_mapping_count = template."event-mapping".size()

            HashMap<String, W2k3CoreParser> tmpCoreParsers = new HashMap<>()
            template."core-mapping".each { attr_mapping ->
                W2k3CoreParser cp = new W2k3CoreParser(attr_mapping, logger)
                tmpCoreParsers.put(cp.type, cp)
            }

            template."event-mapping".each { attr_mapping ->
                W2k3ExtParser p = new W2k3ExtParser(attr_mapping, logger)
                W2k3Parser parser = new W2k3Parser(p.eventID, p, tmpCoreParsers.get(p.type), logger)
                hmParsers.put(parser.eventID, parser)
            }
        }
        return hmParsers.get(eventID)
    }
}

class W2k3IPAddress {
    static final long MS_HOUR = 3600000

    String host_name
    String ip_address
    long expiration = 0

    void refreshIP() {
        try {
            ip_address = InetAddress.getByName(host_name).getHostAddress()
        } catch (Exception e) {
            ip_address = "0.0.0.0"
        }
        expiration = System.currentTimeMillis() + MS_HOUR
    }

    boolean isExpired() {
        return System.currentTimeMillis() > expiration
    }
}

class W2k3HostIPAddresses {
    static final Map<String, W2k3IPAddress> host_ip_map = new HashMap<>()

    synchronized static String getIP(String host) {
        W2k3IPAddress ipa = host_ip_map.get(host)
        if (!ipa) {
            ipa = new W2k3IPAddress()
            ipa.host_name = host
            host_ip_map.put(host, ipa)
        }
        if (ipa.isExpired()) {
            ipa.refreshIP()
        }
        return ipa.ip_address
    }


}

class W2k3ExtData {
    String event_key
    String event_id
    String event_date
    String attr_name
    String value
}

class W2k3CoreData {
    String event_key
    String event_id
    String event_date
    String host_domain
    String host_ip
    String log_type
    String logfile_name
    String log_provider
    String event_time
    String host_name
}

String eventID (String eventXML) { 
    eventXML.find(W2k3Const.common_event_id_regex) 
}


def flowFileList = session.get(W2k3Const.FLOW_BATCH_SIZE)

if (!flowFileList.isEmpty()) {
    flowFileList.each { flowFile ->

        session.removeAttribute(flowFile, "eventparser.exception")
        session.removeAttribute(flowFile, "eventparser.stacktrace" )
        session.removeAttribute(flowFile, "fail-reason")
        session.removeAttribute(flowFile, "fail-message")
        session.removeAttribute(flowFile, "fail-stack")

        line_no = "flowFileLoop.1"
        //println("line no: " + line_no)

        def filename = flowFile.getAttribute("filename")
        def originalLogName
        if (flowFile.getAttributes().containsKey('original_logname')) {
            originalLogName = flowFile.getAttribute('original_logname')
        }   else if (flowFile.getAttributes().containsKey('original-filename')) {
                    originalLogName = flowFile.getAttribute('original-filename')
        } else {
            originalLogName = filename
        }

        FlowFile core_data_flowfile
        List<FlowFile> ext_flowfiles

        //assume the flow file contains only one event
        String the_event_id = 'unknown'
        try {
            String raw_log_data = ''
            //read the flow file into the log body
            session.read(flowFile, { inputStream ->
                raw_log_data = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
            } as InputStreamCallback)

            def log_body = "<?xml version=\"1.1\"?>\n" + raw_log_data.replaceAll(Const.CTRL_CHARS, ' ')
            the_event_id = eventID(log_body)

            W2k3Parser p = W2k3Parsers.getParser(the_event_id, log)
            if (p != null) {
                if (!p.blocked()) {
                    line_no = "flowFileLoop.2"
                    //println("line no: " + line_no)

                    //---------------- CORE DATA FIRST --------------//
                    W2k3CoreData cd = p.getCoreDataObj(log_body)
                    cd.logfile_name = originalLogName //this isn't avail to the parser so add it here

                    String core_json = JsonOutput.toJson(cd)

                    core_data_flowfile = session.create()
                    session.putAttribute(core_data_flowfile, "data_type", "core-data")
                    session.putAttribute(core_data_flowfile, "event_id", cd.event_id)
                    session.putAttribute(core_data_flowfile, "original-filename", originalLogName)
                    session.putAttribute(core_data_flowfile, "hdfs_folder", "/winlog-coredata/event_date=" + cd.event_date + "/event_id=" + cd.event_id)

                    session.write(core_data_flowfile, { outputStream ->
                        outputStream.write(core_json.getBytes(StandardCharsets.UTF_8))
                    } as OutputStreamCallback)
                    line_no = "flowFileLoop.3"

                    //---------------- EXT DATA  --------------//

                    ArrayList<W2k3ExtData> extDataObjs = p.getExtDataObjs(log_body, cd)
                    line_no = "flowFileLoop.3.b"
                    ext_flowfiles = new ArrayList<>()
                    extDataObjs.each { extDataObj ->
                        String ext_json = JsonOutput.toJson(extDataObj)
                        FlowFile ext_data_flowfile = session.create()
                        ext_flowfiles.add(ext_data_flowfile)
                        session.putAttribute(ext_data_flowfile, "event_id", extDataObj.event_id)
                        session.putAttribute(ext_data_flowfile, "data_type", "ext-data")
                        session.putAttribute(ext_data_flowfile, "original-filename", originalLogName)
                        session.putAttribute(ext_data_flowfile, "hdfs_folder", "/winlog-extdata2/event_date=" + extDataObj.event_date + "/event_id=" + extDataObj.event_id  + "/attr_name=" + extDataObj.attr_name)
                        session.write(ext_data_flowfile, { outputStream ->
                            outputStream.write(ext_json.getBytes(StandardCharsets.UTF_8))
                        } as OutputStreamCallback)
                    }
                    line_no = "flowFileLoop.4.0"

                    // add raw data text as ext-data name 'original_string'
                    W2k3ExtData objRawData = new W2k3ExtData()
                    objRawData.with {
                       //copy of key elements from core data
                       event_key = cd.event_key
                       event_id = cd.event_id
                       event_date = cd.event_date
                       attr_name = "original_string"
                       value = log_body
                    }
                    if(!cd.event_id || !objRawData.event_id){
                        throw new Exception("cd.event_id is "+cd.event_id+" rawdata.event_id is " + objRawData.event_id  + " event_id "  + the_event_id  )
                    }
                    line_no = "flowFileLoop.4.1"
                    
                    String raw_json = JsonOutput.toJson(objRawData)
                    FlowFile raw_data_flowfile = session.create()
                    ext_flowfiles.add(raw_data_flowfile)
                    session.putAttribute(raw_data_flowfile, "event_id", objRawData.event_id)
                    session.putAttribute(raw_data_flowfile, "data_type", "ext-data")
                    session.putAttribute(raw_data_flowfile, "original-filename", originalLogName)
                    session.putAttribute(raw_data_flowfile, "hdfs_folder", "/winlog-extdata2/event_date=" + objRawData.event_date + "/event_id=" + objRawData.event_id  + "/attr_name=" + objRawData.attr_name)
                    session.write(raw_data_flowfile, { outputStream ->
                        outputStream.write(raw_json.getBytes(StandardCharsets.UTF_8))
                    } as OutputStreamCallback)
 
                    // end raw data

                    //transfer all
                    //println("transfer all : with "  + ext_flowfiles.size()  + " extdata files")
                    session.transfer(core_data_flowfile, ExecuteScript.REL_SUCCESS)
                    session.transfer(ext_flowfiles, ExecuteScript.REL_SUCCESS)
                    session.remove(flowFile)

                } else {
                    //println("BLOCKED")
                    session.putAttribute(flowFile, "fail-reason", "BLOCKED")
                    session.putAttribute(flowFile, "event-id", the_event_id)
                    session.transfer(flowFile, ExecuteScript.REL_FAILURE)
                }

            } else {
                //println("NO PARSER")
                session.putAttribute(flowFile, "fail-reason", "NO PARSER FOUND")
                session.putAttribute(flowFile, "event-id", the_event_id)
                session.transfer(flowFile, ExecuteScript.REL_FAILURE)
            }
        } catch (Throwable e) {
            def msg = e.getMessage()
            def stack = Arrays.toString(e.getStackTrace())
            log.warn("GOT AN ERROR.  LAST LINE WAS {}.   ERROR IS {}\n{}",[filename, line_no, msg,stack] as Object[])
            session.putAttribute(flowFile, "fail-reason", "ERROR")
            session.putAttribute(flowFile, "event-id", the_event_id)
            session.putAttribute(flowFile, "fail-message", msg)
            session.putAttribute(flowFile, "eventparser.stacktrace",  stack)
            session.transfer(flowFile, ExecuteScript.REL_FAILURE)

            try {
                if( core_data_flowfile ){
                    session.remove(core_data_flowfile)
                }
            } catch (Exception ignore) {
            }
            try {
                if( ext_flowfiles ) {
                    session.remove(ext_flowfiles)
                }
            } catch (Exception ignore) {
            }
        }
    }//end flowfile for each loop
}//end flowfile if empty check
