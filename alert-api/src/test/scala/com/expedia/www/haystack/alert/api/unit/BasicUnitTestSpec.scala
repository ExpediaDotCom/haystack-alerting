/*
 *               Copyright 2018 Expedia, Inc.
 *
 *        Licensed under the Apache License, Version 2.0 (the "License");
 *        you may not use this file except in compliance with the License.
 *        You may obtain a copy of the License at
 *
 *            http://www.apache.org/licenses/LICENSE-2.0
 *
 *        Unless required by applicable law or agreed to in writing, software
 *        distributed under the License is distributed on an "AS IS" BASIS,
 *        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *        See the License for the specific language governing permissions and
 *        limitations under the License.
 *
 */

package com.expedia.www.haystack.alert.api.unit


import org.apache.http.ProtocolVersion
import org.scalatest.{GivenWhenThen, Matchers, WordSpec}

trait BasicUnitTestSpec extends WordSpec with GivenWhenThen with Matchers {

  implicit val executor = scala.concurrent.ExecutionContext.global
  val PROTOCOL_VERSION = new ProtocolVersion("http", 1, 1)
  val EXCEPTION = "failed to connect"



}
