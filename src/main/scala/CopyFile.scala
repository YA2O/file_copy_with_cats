import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._

import java.io._

object CopyFile extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    for {
      _ <-
        if (args.length < 2)
          IO.raiseError(
            new IllegalArgumentException("Need origin and destination files")
          )
        else IO.unit
      orig = new File(args(0))
      dest = new File(args(1))
      count <- copy[IO](orig, dest)
      _ <- IO(
        println(s"$count bytes copied from ${orig.getPath} to ${dest.getPath}")
      )
    } yield ExitCode.Success
  }

  def copy[F[_]: Concurrent](origin: File, destination: File): F[Long] = {
    for {
      guard <- Semaphore[F](1)
      count <- createInAndOutStreams(origin, destination, guard).use {
        case (in, out) => guard.withPermit { transfer(in, out) }
      }
    } yield count
  }

  def createInAndOutStreams[F[_]: Sync](
      in: File,
      out: File,
      guard: Semaphore[F]
  ): Resource[F, (InputStream, OutputStream)] = {
    for {
      inStream <- createInputStream(in, guard)
      outStream <- createOutputStream(out, guard)
    } yield (inStream, outStream)
  }

  def createInputStream[F[_]: Sync](
      f: File,
      guard: Semaphore[F]
  ): Resource[F, FileInputStream] =
    Resource.make {
      Sync[F].delay(new FileInputStream(f))
    } { inStream =>
      guard.withPermit {
        Sync[F].delay(inStream.close()).handleErrorWith(_ => Sync[F].unit)
      }
    }

  def createOutputStream[F[_]: Sync](
      f: File,
      guard: Semaphore[F]
  ): Resource[F, FileOutputStream] =
    Resource.make {
      Sync[F].delay(new FileOutputStream(f))
    } { outStream =>
      guard.withPermit {
        Sync[F].delay(outStream.close()).handleErrorWith(_ => Sync[F].unit)
      }
    }

  def transfer[F[_]: Sync](origin: InputStream, destination: OutputStream): F[Long] = {
    for {
      buffer <- Sync[F].delay(
        new Array[Byte](1024 * 10)
      ) // Allocated only when the IO is evaluated
      total <- transmit(origin, destination, buffer, 0L)
    } yield total
  }

  def transmit[F[_]: Sync](
      origin: InputStream,
      destination: OutputStream,
      buffer: Array[Byte],
      acc: Long
  ): F[Long] =
    for {
      amount <- Sync[F].delay(origin.read(buffer, 0, buffer.length))
      count <-
        if (amount > -1)
          Sync[F].delay(destination.write(buffer, 0, amount)) >> transmit(
            origin,
            destination,
            buffer,
            acc + amount
          )
        else
          // End of read stream reached (by java.io.InputStream contract), nothing to write
          Sync[F].pure(acc)
    } yield count // Returns the actual amount of bytes transmitted

}
