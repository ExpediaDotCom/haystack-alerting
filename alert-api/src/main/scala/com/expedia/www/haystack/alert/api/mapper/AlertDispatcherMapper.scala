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

package com.expedia.www.haystack.alert.api.mapper

import com.expedia.alertmanager._
import com.expedia.open.tracing.api.subscription.{DispatchType, Dispatcher}

trait AlertDispatcherMapper {

  def mapAlertDispatcher(dispatchers: List[Dispatcher]): List[model.Dispatcher] = {
    dispatchers.map(dispatcher => {
      val alertDispatcher = new model.Dispatcher
      alertDispatcher.setEndpoint(dispatcher.getEndpoint)
      alertDispatcher.setType(getDispatcherType(dispatcher.getType))
      alertDispatcher
    })
  }

  def mapDispatchers(dispatchers: List[model.Dispatcher]): List[Dispatcher] = {
    dispatchers.map(dispatcher => {
      Dispatcher.newBuilder().setEndpoint(dispatcher.getEndpoint).setType(getDispatcherType(dispatcher.getType)).build()
    })
  }

  private def getDispatcherType(dispatchType: DispatchType): model.Dispatcher.Type = {
    (dispatchType: @unchecked) match {
      case DispatchType.EMAIL => model.Dispatcher.Type.EMAIL
      case DispatchType.SLACK => model.Dispatcher.Type.SLACK
    }
  }

  private def getDispatcherType(dispatchType: model.Dispatcher.Type): DispatchType = {
    dispatchType match {
      case model.Dispatcher.Type.EMAIL => DispatchType.EMAIL
      case model.Dispatcher.Type.SLACK => DispatchType.SLACK
    }
  }

}
