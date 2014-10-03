/*
 * TODO: License goes here!
 */

package sa.com.mobily.model

/**
  */
case class CSEvent(begin_time: Long,
                   ci: String,
                   eci: String,
                   end_time: Long,
                   event_type: Short,
                   imei: Long,
                   imsi: Long,
                   ixc: String,
                   lac: String,
                   mcc: String,
                   mnc: String,
                   msisdn: Long,
                   rac: String,
                   rat: Short,
                   sac: String,
                   tac: String) {

}


/**
  */
case class PSEvent(begin_time: Long,
                   ci: String,
                   eci: String,
                   end_time: Long,
                   event_type: Short,
                   imei: Long,
                   imsi: Long,
                   ixc: String,
                   lac: String,
                   mcc: String,
                   mnc: String,
                   msisdn: Long,
                   prot_category: Short,
                   prot_type: Short,
                   rac: String,
                   rat: Short,
                   sac: String,
                   server_ip: String,
                   server_port: Short,
                   tac: String) {

}


object Event {


}