package toolkits

/**
  *
  * @author jingxuan
  * @since 2019-10-30
  *
  */
import scala.math.BigInt

object Schemas {
  case class DenormLinkTravelLocation(
   agent_gender: String,
   agent_home_gcc: String,
   agent_home_gcc_name: String,
   agent_home_poa: String,
   agent_home_sa1: String,
   agent_home_sa2: String,
   agent_home_sa2_name: String,
   agent_home_sa3: String,
   agent_home_sa3_name: String,
   agent_home_sa4: String,
   agent_home_sa4_name: String,
   agent_home_state: String,
   agent_home_state_name: String,
   agent_id: String,
   agent_work_gcc: String,
   agent_work_gcc_name: String,
   agent_work_poa: String,
   agent_work_sa1: String,
   agent_work_sa2: String,
   agent_work_sa2_name: String,
   agent_work_sa3: String,
   agent_work_sa3_name: String,
   agent_work_sa4: String,
   agent_work_sa4_name: String,
   agent_work_state: String,
   agent_work_state_name: String,
   agent_year_of_birth: String,
   country_code: String,
   country_name: String,
   destination_building: String,
   destination_gcc: String,
   destination_gcc_name: String,
   destination_poa: String,
   destination_sa1: String,
   destination_sa2: String,
   destination_sa2_name: String,
   destination_sa3: String,
   destination_sa3_name: String,
   destination_sa4: String,
   destination_sa4_name: String,
   destination_state: String,
   destination_state_name: String,
   destination_type: String,
   distance: Double,
   duration: BigInt,
   end_time: String,
   origin_building: String,
   origin_gcc: String,
   origin_gcc_name: String,
   origin_poa: String,
   origin_sa1: String,
   origin_sa2: String,
   origin_sa2_name: String,
   origin_sa3: String,
   origin_sa3_name: String,
   origin_sa4: String,
   origin_sa4_name: String,
   origin_state: String,
   origin_state_name: String,
   origin_type: String,
   start_time: String,
   direction: String,
   link_id: String,
   mode: String,
   trip_end_time: String,
   trip_start_time: String)
}
