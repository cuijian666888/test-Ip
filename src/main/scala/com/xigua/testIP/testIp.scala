package com.xigua.testIP

import org.apache.spark.{SparkConf, SparkContext}

/*
*  测试写入ip之后是否超出对应的网段
* */
object testIp {
  def main(args: Array[String]): Unit = {

    //配置上下文环境对象
    val conf = new SparkConf().setAppName("judgeIPisSameAddress").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //读取文件中的记录
    val ipTestTxt = sc.textFile("D:\\idea\\work_project\\test-Ip\\src\\main\\resources\\unallow.txt")


    //定义mask地址
    val DEFAULT_SUBNET_MASK_A: String = "255.0.0.0";
    val DEFAULT_SUBNET_MASK_B: String = "255.255.0.0";
    val DEFAULT_SUBNET_MASK_C: String = "255.255.255.0";
    //定义ip类型
    val TYPE_IP_A: String = "A";
    val TYPE_IP_B: String = "B";
    val TYPE_IP_C: String = "C";
    val TYPE_IP_D: String = "D";
    val TYPE_IP_LOCATE: String = "locate"


    def test() {
      var ip_a = "127.0.01.15";
      var ip_c = "192.168.1.1";
      var binaryIp = getBinaryIp("192.168.1.1");
      System.out.println(getIpType(ip_a));

    }

    //判断ip是否属于同一网段，默认子网掩码
    def isIPSameAddress(resourceIp: String, requestIp: String) = {
      if (getIpType(resourceIp).equals(getIpType(requestIp))) {
        isSameAddress(resourceIp, requestIp, getIpDefaultMask(getIpType(resourceIp)));
      }
      false;
    }

    //通过ip类型，获取默认IP子网掩码
    def getIpDefaultMask(ipType: String) = ipType match {
      case TYPE_IP_A => DEFAULT_SUBNET_MASK_A
      case TYPE_IP_B => DEFAULT_SUBNET_MASK_B
      case TYPE_IP_C => DEFAULT_SUBNET_MASK_C
      case _ => null
    }

    //判断ip是否属于同一网段
    def isSameAddress(resourceIp: String, requestIp: String, subnetMask: String) {
      var resourceAddr = getAddrIp(resourceIp, subnetMask);
      var subnetMaskAddr = getAddrIp(requestIp, subnetMask);
      if (resourceAddr.equals(subnetMaskAddr)) {
        return true;
      }
      return false;
    }

    //获取ip的二进制字符串
    def getBinaryIp(data: String) = {
      var datas = data.split("\\.")
      var binaryIp = "";
      for (ipStr <- 0 until datas.length) {
        var signIp = ipStr.toLong
        var binary = signIp.toLong;
        var binaryInt = binary.toLong;
        binaryIp += binaryInt.toByte

      }
      binaryIp;
    }

    //获取ip的地址位
    def getAddrIp(ip: String, subnetMask: String) {
      var addrIp = new StringBuilder()
      var binaryIp = getBinaryIp(ip)
      var binarySubnetMask = getBinaryIp(subnetMask)
      for (i <- 1 to 32) {
        var ipByte = binaryIp.toString.charAt(i).toByte
        val subnetMaskByte = binarySubnetMask.toString.charAt(i).toByte
        addrIp.append(ipByte & subnetMaskByte)
        return addrIp.toString()
      }
    }

    //获取ip是什么类型，ABCD
    def getIpType(ip: String) = {
      var binaryIp = getBinaryIp(ip)
      System.out.println(binaryIp)
      if (ip.startsWith("127")) {
        TYPE_IP_LOCATE
      }
      if (binaryIp.toString.startsWith("0")) {
        TYPE_IP_A
      }
      if (binaryIp.toString.startsWith("10")) {
        TYPE_IP_B
      }
      if (binaryIp.toString.startsWith("110")) {
        TYPE_IP_C
      }
      if (binaryIp.toString.startsWith("1110")) {
        TYPE_IP_D
      }
      "ip异常"
    }

    //从文件中读取出来之后，进行ip判断
    //ip从文件中读取出来之后转变成数组
    val ipArray = ipTestTxt.collect()
    //对数组进行遍历
    for (ip <- 0 until ipArray.length) {
      if (ipArray.contains(args(0))) {
        print("ip进行验证" -> isIPSameAddress(ip.toString, args(0)))
      }
      print("ip无法进行验证")
    }
  }
}
