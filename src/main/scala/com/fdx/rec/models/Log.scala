package com.fdx.rec.models

case class Log(
                itemId: Int,
                userId: String,
                action: String,
                vTime: String
              ) {}