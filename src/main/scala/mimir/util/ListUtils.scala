package mimir.util;

object ListUtils {
  // There's gotta be a primitive for this...
  // Temporary placeholder for now.
  def powerList[A](l: List[List[A]]): List[List[A]] =
    if(l.isEmpty){
      List(List[A]())
    } else {
      powerList[A](l.tail).flatMap( (rest) =>
        l.head.map( (head) =>
          (head :: rest)
        )
      )
    } 

  def unzip[A](l: List[List[A]]): List[List[Option[A]]] =
  {
    // Only run if at least one nested list is non-empty
    if(l.exists( _.length > 0 )){
      // unzip one layer of nested maps
      val (hd, tl) = l.map({
        case hd :: tl => (Some(hd), tl)
        case Nil => (None, Nil)
      }).unzip
      // and recur
      hd :: unzip(tl)
    } else {
      Nil
    }
  }
}