package com.totango.prototype.realtimeupdater

data class Payload(val id: String, val retries: Int, val sendDelay: Int, val info: String)