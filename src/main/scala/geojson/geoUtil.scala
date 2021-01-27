package geojson

import com.esri.core.geometry.{Geometry, OperatorExportToWkt, OperatorImportFromJson, WktExportFlags}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.functions.udf

/**
  *
  * @author jingxuan
  * @since 2019-08-29
  *
  */

object geoUtil {
  def sizeDiff(newGeo: String, oldGeo: String): Double = {
    lazy val reader = new WKTReader()
    val areaDiff = reader.read(newGeo).getArea - reader.read(oldGeo).getArea
    math.abs(areaDiff) / reader.read(oldGeo).getArea
  }

  val sizeDiffUDF = udf[Double, String, String](sizeDiff)

  def toWKT(geo: String): String = {
    try{
      val replacedInput = geo.replace("\\", "")
      val inputPolygon = OperatorImportFromJson.local()
        .execute(Geometry.Type.Polygon, replacedInput).getGeometry()
      OperatorExportToWkt.local().execute(WktExportFlags.wktExportDefaults, inputPolygon,
        null)
    }catch{
      case e: Exception => "POLYGON EMPTY"
    }
  }

  val toWKTUDF = udf[String, String](toWKT)
}
