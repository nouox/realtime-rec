package com.fdx.rec.utils

import java.util.ResourceBundle

object MyPropsUtils {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String): String = {
    bundle.getString(propsKey)
  }
}
