package org.sunbird.dp.certificatededuplication.domain

import java.util


import org.sunbird.dp.core.domain.Events

class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  def dedupKey: String = s"CERT:$mid"
  def primaryCategory: String = telemetry.read[String]("edata.primaryCategory").orNull
  def orgId: String = telemetry.read[String]("edata.orgId").orNull
  def userId: String = telemetry.read[String]("edata.userId").orNull
  def courseName: String = telemetry.read[String]("edata.courseName").orNull
  def courseCategory: String = telemetry.read[String]("edata.courseCategory").orNull
  def issuedDate: String = telemetry.read[String]("edata.issuedDate").orNull
  def batchId: String = telemetry.read[String]("edata.related.batchId").orNull
  def courseId: String = telemetry.read[String]("edata.related.courseId").orNull

  def toFilteredMap: util.Map[String, AnyRef] = {
    val filtered = new util.LinkedHashMap[String, AnyRef]()
    filtered.put("mid", mid)
    filtered.put("primaryCategory", primaryCategory)
    filtered.put("orgId", orgId)
    filtered.put("userId", userId)
    filtered.put("courseName", courseName)
    filtered.put("courseCategory", courseCategory)
    filtered.put("issuedDate", issuedDate)
    filtered.put("batchId", batchId)
    filtered.put("courseId", courseId)
    filtered
  }
}