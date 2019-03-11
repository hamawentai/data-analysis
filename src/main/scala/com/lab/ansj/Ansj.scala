package com.lab.ansj

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}

import org.ansj.app.keyword.Keyword
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.JavaConverters

object Ansj {
  def main(args: Array[String]): Unit = {
//    val str = "老汉奸杀了我们两个兄弟"
    val str = "人 工智能是一  门极富挑战性的科学，从事这项工作的人必须懂得计算机知识，心理学和哲学。人工智能是包括十分广泛的科学，它由不同的领域组成，如机器学习，计算机视觉等等，总的说来，人工智能研究的一个主要目标是使机器能够胜任一些通常需要人类智能才能完成的复杂工作。但不同的时代、不同的人对这种“复杂工作”的理解是不同的"
    val words = ToAnalysis.parse(str).getTerms

    val trems: List[Term] = JavaConverters.asScalaIteratorConverter(words.iterator()).asScala.toList
    trems.foreach(trem => if (trem.getNatureStr.contains("n")&&trem.getNatureStr!="null") println(trem.getName+","+trem.getNatureStr))
//    println(words)
//    val reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(str.getBytes())))
//    val key = Tool.getComputer(new ToAnalysis(), 15)
//    val it = key.computeArticleTfidf(str)
//    val itr: List[Keyword] = JavaConverters.asScalaIteratorConverter(it.iterator()).asScala.toList
//    itr.foreach(it => println(it))
  }

}
