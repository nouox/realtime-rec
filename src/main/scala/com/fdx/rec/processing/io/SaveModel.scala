package com.fdx.rec.processing.io

import com.fdx.rec.utils.MyConstants
import org.apache.spark.ml.recommendation.ALSModel

object SaveModel {
  def save(cfModel: ALSModel, cbModel: ALSModel): Unit = {
    cfModel.write.save(MyConstants.cfModelPath)
    cbModel.write.save(MyConstants.cbModelPath)
  }
}
