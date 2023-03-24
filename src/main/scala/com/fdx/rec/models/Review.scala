package com.fdx.rec.models

case class Review(
                   itemId: Integer,
                   userId: String,
                   feedback: String,
                   gmtCreate: String
                 ) {}
