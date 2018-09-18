import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.script.ScriptingComponentUtils

import javax.xml.parsers.SAXParser
import javax.xml.parsers.SAXParserFactory
import java.nio.charset.StandardCharsets
import groovy.json.JsonOutput
import groovy.util.slurpersupport.GPathResult

import java.util.regex.Pattern


class Const {

    static Pattern eventIDPattern = Pattern.compile("(?<=<EventID>)(.*)(?=<\\/EventID>)")
    static Pattern CTRL_CHARS   = Pattern.compile('\\p{Cntrl}')
    static final Set<String> FilteredAttributes = new HashSet<String>() {
        {

            add("KEYLENGTH")
            add("LMPACKAGENAME")
            add("OBJECTSERVER")
            add("PRIVILEGELIST")
            add("AUTHENTICATIONPACKAGENAME")
            add("RESTRICTEDSIDCOUNT")
            add("APPCORRELATIONID")
            add("ATTRIBUTESYNTAXOID")
            add("CERTISSUERNAME")
            add("CERTSERIALNUMBER")
            add("CERTTHUMBPRINT")
            add("ENDUSN")
            add("FILTERRTID")
            add("HANDLEID")
            add("LAYERNAME")
            add("LAYERRTID")
            add("OPCORRELATIONID")
            add("PROCESSID")
            add("REMOTEMACHINEID")
            add("REMOTEUSERID")
            add("SUBJECTLOGONID")
            add("TARGETLOGONID")
            add("TICKETENCRYPTIONTYPE")
            add("TICKETOPTIONS")
            add("TRANSMITTEDSERVICES")
            add("ACCESSMASK")
            add("ACCESSREASON")
            add("ACCESSLIST")
            add("TRANSACTIONID")
        }

    };

    static final Set<String> FilteredEventIDs = new HashSet<String>() {
        {
            add("5156") //The Windows Filtering Platform has permitted a connection
            add("4675") //SIDS were filtered
            add("4769") //A Kerberos service ticket was requested.
            add("5158") //The Windows Filtering Platform has permitted a bind to a local port

            add("4928") //49xx are all active directory replication events.
            add("4929")
            add("4930")
            add("4931")
            add("4932")
            add("4933")
            add("4934")
            add("4935")
            add("4936")
            add("4937")
}
    };
}


class IPAddress {
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

class HostIPAddresses {
    static final Map<String, IPAddress> host_ip_map = new HashMap<>()

    synchronized static String getIP(String host) {
        IPAddress ipa

        if (host_ip_map.get(host) == null) {
            ipa = new IPAddress()
            ipa.host_name = host
            host_ip_map.put(host, ipa)

        } else {
            ipa = host_ip_map.get(host)
        }


        if (ipa.isExpired())
            ipa.refreshIP()


        return ipa.ip_address

    }


}

class ExtData {

    String event_key
    String event_id
    String event_date
    String attr_name
    String value
}

class CoreData {

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


}//end CoreData


class ParserFactory {
    static SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser ;
    SAXParser getParser() {
      if (null == parser ) {
          parser =  factory.newSAXParser()
      }
      return parser
    }
}


int flow_batch_size = 10

try {
    flow_batch_size = Integer.parseInt(FLOW_BATCH_SIZE.value)
} catch (Exception ignore) {
}

ProcessSession the_session = session

log.info("FLOW BATCH SIZE : " + flow_batch_size)

def flowFileList = the_session.get(flow_batch_size)

if (!flowFileList.isEmpty()) {
    SAXParser saxParser = new ParserFactory().getParser()
    flowFileList.each { flowFile ->
        /*
        <li> get the xml, parse it with the groovy XMLSlurper
        <li> use gpath to get all the attribute values we want
        <li>  put those values in the two classes, coredata and auxdata
        <li>  use the groovy JSONOutput parser to turn those into json
        <li>  put those on the flow files and we are done
         */
        String log_body = ''
        try {
            //read the flow file into the log body
            the_session.read(flowFile, { inputStream ->
                log_body = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
            } as InputStreamCallback)

            log_body=log_body.trim()
            log_body=log_body.substring(0, log_body.lastIndexOf("</Event>") + "</Event>".size() )

            // odds are that if we dont bother to parse the majority of messages, we will save time. just a quick regex for eventID's we dont want
            String v_event_id = log_body.find(Const.eventIDPattern)

            // send all kinds of errors to failure since we only have 2 options: failure and succeed;
            // keep success exclusively for real data
            if(flowFile.getAttribute('event_count') == '0' ) {
                the_session.transfer(flowFile, ScriptingComponentUtils.REL_FAILURE)
            }
            else if (log_body.size() < 100){
                the_session.putAttribute(flowFile,'event_count','0')
                the_session.transfer(flowFile, ScriptingComponentUtils.REL_FAILURE)
            }
            else if (null == v_event_id || Const.FilteredEventIDs.contains(v_event_id)) {
                the_session.putAttribute(flowFile, "filtered", "true")
                the_session.putAttribute(flowFile, "event_id", v_event_id)
                the_session.transfer(flowFile, ScriptingComponentUtils.REL_FAILURE)
            } else  {
                // process it
                log_body = "<?xml version=\"1.1\"?>\n" + log_body.replaceAll(Const.CTRL_CHARS,' ')

                //parse the xml with  gpath
                GPathResult gpr = new XmlSlurper(saxParser).parseText(log_body)

                //instantiate CoreData and ExtData
                //instantiate ExtData here is because the first three properties will be common -
                // -just set key/value later.
                CoreData coreDataObj = new CoreData()
                coreDataObj.with {
                    event_key = UUID.randomUUID().toString() + "-" + gpr.System.EventRecordID.text()
                    event_id = v_event_id
                    event_time = gpr.System.TimeCreated.@SystemTime.text()
                    log_provider = gpr.System.Provider.@Name.text()
                    host_name = gpr.System.Computer.text()
                    host_ip = HostIPAddresses.getIP(host_name)
                    host_domain = (host_name.indexOf('.') > -1) ? host_name.substring(host_name.indexOf(".") + 1) : ""
                    //log_type = HostLogTypes.getLogType(host_name,"windows-security-log")
                    log_type="null"
                    event_date = event_time.substring(0, 10)

                    if (flowFile.getAttributes().containsKey("original_logname"))
                        logfile_name = flowFile.getAttribute("original_logname")
                    else
                        logfile_name = flowFile.getAttribute("filename")
                }

                ExtData objExtData = new ExtData()
                objExtData.with {
                    //just a copy of these three elements
                    event_key = coreDataObj.event_key
                    event_id = coreDataObj.event_id
                    event_date = coreDataObj.event_date
                }

                /*
                "ext data" is any data that is variable from event to event thus is not core data which is always there on every event
                 */

                int data_count = gpr.EventData.Data.size()
                List<FlowFile> ext_flowfiles = new ArrayList<>()

                for (c = 0; c < data_count; c++) {
                    String value = gpr.EventData.Data[c].text()
                    String name = gpr.EventData.Data[c].@Name.text().replace("-TEMP2", "").replace("-TEMP", "")


                    if (!(value.isEmpty() || value == "-" || name.endsWith("Sid") || name.toUpperCase().endsWith("GUID") ||
                            Const.FilteredAttributes.contains(name.toUpperCase()))) {
                        FlowFile ext_data_flowfile
                        try {

                            objExtData.attr_name = name
                            objExtData.value = value

                            String ext_json = JsonOutput.toJson(objExtData)
                            String hdfs_folder =  "/winlog-extdata2/event_date=" + objExtData.event_date + "/event_id=" + objExtData.event_id  + "/attr_name=" + objExtData.attr_name
                            ext_data_flowfile = the_session.create()
                            ext_data_flowfile = the_session.write(ext_data_flowfile, { outputStream ->

                                outputStream.write(ext_json.getBytes(StandardCharsets.UTF_8))

                            } as OutputStreamCallback)

                            //there is an issue with filenames not being unique - so create a unique filename here
                            String filename = UUID.randomUUID().toString() + "-" + System.currentTimeMillis()
                            the_session.putAttribute(ext_data_flowfile, "filename", filename)
                            the_session.putAttribute(ext_data_flowfile, "data_type", "ext-data")
                            the_session.putAttribute(ext_data_flowfile, "hdfs_folder", hdfs_folder)
                            ext_flowfiles.add(ext_data_flowfile)


                        } catch (Exception adf) {
                            log.error(adf.getMessage() + "\n" + objExtData.toString());
                            if (ext_flowfiles.size() > 0) {
                                the_session.remove(ext_flowfiles)
                            }

                            the_session.transfer(flowFile, REL_FAILURE)
                            return
                        }
                    }
                }//end for/next
                
                
                // original string  as extData
               FlowFile raw_data_flowfile
               try {
                   
                   objExtData.attr_name = "original_string"
                   objExtData.value = log_body

                   String ext_json = JsonOutput.toJson(objExtData)
                   String hdfs_folder =  "/winlog-extdata2/event_date=" + objExtData.event_date + "/event_id=" + objExtData.event_id  + "/attr_name=" + objExtData.attr_name
                   raw_data_flowfile = the_session.create()
                   raw_data_flowfile = the_session.write(raw_data_flowfile, { outputStream ->

                       outputStream.write(ext_json.getBytes(StandardCharsets.UTF_8))

                   } as OutputStreamCallback)

                   //there is an issue with filenames not being unique - so create a unique filename here
                   String filename = UUID.randomUUID().toString() + "-" + System.currentTimeMillis()
                   the_session.putAttribute(raw_data_flowfile, "filename", filename)
                   the_session.putAttribute(raw_data_flowfile, "data_type", "ext-data")
                   the_session.putAttribute(raw_data_flowfile, "hdfs_folder", hdfs_folder)
                   ext_flowfiles.add(raw_data_flowfile)

               } catch (Exception adf) {
                   log.error(adf.getMessage() + "\n" + objExtData.toString());
                   if (ext_flowfiles.size() > 0) {
                       the_session.remove(ext_flowfiles)
                   }
                   the_session.transfer(flowFile, ScriptingComponentUtils.REL_FAILURE)
                   return
               }
                
                // end original string
                
                
                FlowFile core_data_flowfile
                try {

                    String core_json = JsonOutput.toJson(coreDataObj)
                    core_data_flowfile = the_session.create()
                    String hdfs_folder =  "/winlog-coredata/event_date=" + objExtData.event_date + "/event_id=" + objExtData.event_id
                    core_data_flowfile = the_session.write(core_data_flowfile, { outputStream ->

                        outputStream.write(core_json.getBytes(StandardCharsets.UTF_8))

                    } as OutputStreamCallback)

                    String filename = UUID.randomUUID().toString() + "-" + System.currentTimeMillis()
                    the_session.putAttribute(core_data_flowfile, "filename", filename)
                    the_session.putAttribute(core_data_flowfile, "data_type", "core-data")
                    the_session.putAttribute(core_data_flowfile, "hdfs_folder", hdfs_folder)


                } catch (Exception cff) {

                    log.error(cff.getMessage() + objCoreData.toString())
                    if (core_data_flowfile != null) {
                        the_session.remove(core_data_flowfile)
                    }

                    the_session.transfer(flowFile, ScriptingComponentUtils.REL_FAILURE)
                    return
                }
                //core and all ext-data to success
                the_session.transfer(core_data_flowfile, ScriptingComponentUtils.REL_SUCCESS)
                if (ext_flowfiles.size() > 0) {
                    the_session.transfer(ext_flowfiles, ScriptingComponentUtils.REL_SUCCESS)
                }

                //discard the original flowFile
                the_session.remove(flowFile)
            }

        } catch (Throwable e) {
            log.error(e.getMessage())
            the_session.putAttribute(flowFile, "eventparser.exception", e.getMessage() )
            the_session.putAttribute(flowFile, "eventparser.stacktrace", Arrays.toString(e.getStackTrace()) )
            the_session.transfer(flowFile, ScriptingComponentUtils.REL_FAILURE)
        }


    }//end flowfile for each loop


}//end flowfile if empty check
