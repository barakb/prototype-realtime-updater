package com.totango.prototype.realtimeupdater

data class Payload(var retries: Int, val sendDelay: Int, val info: String)