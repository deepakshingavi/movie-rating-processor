package com.dp.training.model

/**
 *  Note : Class field naming intentionally avoid Java naming convention to match column
 *  name with Data fields
 */
case class Rating(UserID: Int, MovieID: Int, Rating :Double, Timestamp : Long)
