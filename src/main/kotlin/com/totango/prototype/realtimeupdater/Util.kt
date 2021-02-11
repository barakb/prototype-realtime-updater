package com.totango.prototype.realtimeupdater

sealed class Either<out A, out B> {
    companion object {
        fun <A> left(a: A) = Left(a)
        fun <B> right(b: B) = Right(b)
    }

    data class Left<A>(val left: A) : Either<A, Nothing>()
    data class Right<B>(val right: B) : Either<Nothing, B>()
}
