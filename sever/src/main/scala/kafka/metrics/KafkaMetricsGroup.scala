package kafka.metrics

import com.tong.kafka.common.utils.Sanitizer
import com.tong.kafka.server.metrics.KafkaYammerMetrics
import com.yammer.metrics.core._
import kafka.utils.Logging

import java.util.concurrent.TimeUnit

trait KafkaMetricsGroup extends Logging {

  /**
   * Creates a new MetricName object for gauges, meters, etc. created for this
   * metrics group.
   * @param name Descriptive name of the metric.
   * @param tags Additional attributes which mBean will have.
   * @return Sanitized metric name object.
   */
  def metricName(name: String, tags: scala.collection.Map[String, String]): MetricName = {
    val klass = this.getClass
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")

    explicitMetricName(pkg, simpleName, name, tags)
  }


  def explicitMetricName(group: String, typeName: String, name: String,
                                   tags: scala.collection.Map[String, String]): MetricName = {

    val nameBuilder: StringBuilder = new StringBuilder

    nameBuilder.append(group)

    nameBuilder.append(":type=")

    nameBuilder.append(typeName)

    if (name.nonEmpty) {
      nameBuilder.append(",name=")
      nameBuilder.append(name)
    }

    val scope: String = toScope(tags).orNull
    val tagsName = toMBeanName(tags)
    tagsName.foreach(nameBuilder.append(",").append(_))

    new MetricName(group, typeName, name, scope, nameBuilder.toString)
  }

  def newGauge[T](name: String, metric: Gauge[T], tags: scala.collection.Map[String, String] = Map.empty): Gauge[T] =
    KafkaYammerMetrics.defaultRegistry().newGauge(metricName(name, tags), metric)

  def newMeter(name: String, eventType: String, timeUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty): Meter =
    KafkaYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit)

  def newMeter(metricName: MetricName, eventType: String, timeUnit: TimeUnit): Meter =
    KafkaYammerMetrics.defaultRegistry().newMeter(metricName, eventType, timeUnit)

  def newHistogram(name: String, biased: Boolean = true, tags: scala.collection.Map[String, String] = Map.empty): Histogram =
    KafkaYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased)

  def newTimer(name: String, durationUnit: TimeUnit, rateUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty): Timer =
    KafkaYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit)

  def removeMetric(name: String, tags: scala.collection.Map[String, String] = Map.empty): Unit =
    KafkaYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags))

  private def toMBeanName(tags: collection.Map[String, String]): Option[String] = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != "" }
    if (filteredTags.nonEmpty) {
      val tagsString = filteredTags.map { case (key, value) => "%s=%s".format(key, Sanitizer.jmxSanitize(value)) }.mkString(",")
      Some(tagsString)
    }
    else None
  }

  private def toScope(tags: collection.Map[String, String]): Option[String] = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != ""}
    if (filteredTags.nonEmpty) {
      // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
      val tagsString = filteredTags
        .toList.sortWith((t1, t2) => t1._1 < t2._1)
        .map { case (key, value) => "%s.%s".format(key, value.replaceAll("\\.", "_"))}
        .mkString(".")

      Some(tagsString)
    }
    else None
  }

}

object KafkaMetricsGroup extends KafkaMetricsGroup