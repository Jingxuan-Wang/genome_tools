/*
 * Copyright © DataSpark Pte Ltd 2014 - 2017.
 * This software and any related documentation contain confidential and proprietary information of DataSpark and its licensors (if any).
 * Use of this software and any related documentation is governed by the terms of your written agreement with DataSpark.
 * You may not use, download or install this software or any related documentation without obtaining an appropriate licence agreement from DataSpark.
 * All rights reserved.
 */

package timeOp

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, Temporal}

/**
  *
 * @author jingxuan
  * @since 2019-03-20
  *
 */
object TimeCommon {

  private final val dtFormat: String = "yyyy-MM-dd['T'][' ']HH:mm:ss[.][,][SSS][SS][S]"
  private final val strFormat: String = "yyyy-MM-dd HH:mm:ssZZZ"
  private final val timeFormat: String = "HH:mm:ss"
  private final val dateFormat: String = "yyyy-MM-dd"
  private final val lbsDateFormat: String = "yyyy-MM-dd HH:mm:ss"
  final val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dtFormat)
  final val strFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(strFormat)
  final val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
  final val timeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(timeFormat)
  final val lbsDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(lbsDateFormat)
  val input_tz: String = "UTC"

  implicit val dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.by(_.toEpochSecond)

  /**
    * format zdt from String
    * @param strDatetime String in format "yyyy-MM-dd HH:mm:ssZZZ"
    * @return ZonedDateTime
    */
  implicit def zdtFromString(strDatetime: String): ZonedDateTime = {
    val zdt = ZonedDateTime.parse(strDatetime, strFormatter)
    if (zdt.getZone.getId.equals("Z")) zdt.withZoneSameInstant(ZoneId.of("UTC")) else zdt
  }

  implicit def zdtToString(zdt: ZonedDateTime): String = zdt.format(strFormatter)

  implicit def localDateFromString(dateStr: String): LocalDate = LocalDate.parse(dateStr)

  implicit def localDateToString(date: LocalDate): String = date.format(dateFormatter)

  /**
    * parse string into ZonedDatetime in UTC
    * @param str Datetime String
    * @param timeZone Zone String for the original timezone
    * @return ZonedDatetime
    */
  def parseToUTC(str: String, timeZone: String = input_tz): ZonedDateTime =
    timeZone match {
      case "UTC" => parse(str, timeZone)
      case _ =>
        val localDT = parse(str, timeZone)
        toOtherTZ(localDT, "UTC")
    }

  /**
    * parse string into Datetime with taking timeZone into account
    * @param str Datetime String
    * @param timeZone timeZone String
    * @return ZonedDatetime
    */
  def parse(str: String, timeZone: String): ZonedDateTime =
    ZonedDateTime.of(LocalDateTime.parse(str, dtFormatter), ZoneId.of(timeZone))

  /**
    * convert dt from initial timeZone to a different timeZone, for example, convert datetime from utc to
    * local time timeZone
    * @param zdt ZonedDateTime
    * @param timeZone New Zone
    * @return
    */
  def toOtherTZ(zdt: ZonedDateTime, timeZone: String): ZonedDateTime =
    zdt.withZoneSameInstant(ZoneId.of(timeZone))

  /**
    * get Date(in UTC) from ZonedDateTime
    * @param zdt ZonedDateTime
    * @return Date in utc
    */
  def toDate(zdt: ZonedDateTime): String = zdt.format(dateFormatter)

  /**
    * get start of day from ZonedDateTime
    * @param zdt ZonedDateTime
    * @return start of day in ZonedDateTime
    */
  def startOfDay(zdt: ZonedDateTime): ZonedDateTime = zdt.toLocalDate().atStartOfDay(zdt.getZone())

  /**
    * Get the time difference betwen two LocalDateTime or ZonedDateTime(dt2 - dt1)
    * @param dt1 LocalDateTime or ZonedDateTime
    * @param dt2 LocalDateTime or ZonedDateTime
    * @return time difference in Second in Long
    */
  def timeDifference(dt1: Temporal, dt2: Temporal): Long =
    ChronoUnit.SECONDS.between(dt1, dt2)

  /**
    * Rounding the time up/down by given interval
    * @param record record Original datetime
    * @param interval interval setting for rounding
    * @param rdown whether to rdown only or not
    * @return Rounded datetime
    */
  def roundingTime(record: ZonedDateTime, interval: Int = 60, rdown: Boolean = false): ZonedDateTime = {

    if (interval < 1) {
      throw new IllegalArgumentException("interval must be greater than 1 sec")
    }

    val startOfDay = record.truncatedTo(ChronoUnit.DAYS);
    val secondSinceStart = Duration.between(startOfDay, record).getSeconds

    val roundedSecond = rdown match {
      case true => ((secondSinceStart * 1.0) / (interval.toLong * 1.0)).toInt * (interval.toLong * 1.0)
      case _ => Math.round((secondSinceStart * 1.0) / (interval.toLong * 1.0)) * (interval.toLong * 1.0) //Convert second to double
    }
    startOfDay.plusSeconds(roundedSecond.toLong)
  }

  /**
    *
    * @param zdt
    * @return string in yyyy-MM-dd HH:mm:ss format
    */
  def zdtToLbsString(zdt: ZonedDateTime): String = {
    zdt.format(lbsDateFormatter)
  }

  /**
    *
    * @param dt
    * @return string in yyyy-MM-dd HH:mm:ss format
    */
  def localDtToString(dt: LocalDateTime): String = {
    dt.format(lbsDateFormatter)
  }

  def localTimeFromString(strTime: String): LocalTime = {
    LocalTime.parse(strTime, timeFormatter)
  }

  /**
    *
    * @param time
    * @return string in HH:mm:ss format
    */
  def localTimeToString(time: LocalTime): String = {
    time.format(timeFormatter)
  }

  /**
    * convert timestamp in UTC to Local time by given offset
    * (Moved from UTC2Loc() in Utils.scala)
    * @param timestampInUTC: time in UTC
    * @param offset: offset for UTC time to local time
    * @return localTime in LocalDateTime format
    */
  def utcToLocalTime(timestampInUTC: ZonedDateTime, offset: Double): LocalDateTime = {
    val offsetSeconds = (offset * 3600).asInstanceOf[Int]
    LocalDateTime.ofInstant(timestampInUTC.toInstant, ZoneOffset.ofTotalSeconds(offsetSeconds))
  }

  /**
    * creating a date range in local date
    * @param from (LocalDate)
    * @param to (LocalDate)
    * @param step (Int) in day
    * @return
    */
  def dateRange(from: LocalDate, to: LocalDate, step: Int): Iterator[LocalDate] =
    Iterator.iterate(from)(_.plus(step, ChronoUnit.DAYS)).takeWhile(!_.isAfter(to))

  /**
    * convert epoch time to datetime
    * @param longValue
    * @return
    */
  def epochToDT(longValue: Long, timezone: String = "UTC"): LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(longValue), ZoneId.of(timezone))

}
