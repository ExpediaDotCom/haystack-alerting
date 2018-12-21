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
import com.expedia.alertmanager.model.{Field, Operand}
import com.expedia.open.tracing.api.subscription
import com.expedia.open.tracing.api.subscription.ExpressionTree

import scala.collection.JavaConverters._

trait ExpressionTreeMapper {

  def getFieldOperand(operand: subscription.Operand): Operand = {
    val alertOperand = new Operand
    val field = new Field
    field.setKey(operand.getField.getName)
    field.setValue(operand.getField.getValue)
    alertOperand.setField(field)
    alertOperand
  }

  def mapExpressionTree(expressionTree: ExpressionTree): model.ExpressionTree = {
    val expression = new model.ExpressionTree()

    val operands: List[Operand] = expressionTree.getOperandsList.asScala.map(operand => {
      operand.getOperandCase match {
        case com.expedia.open.tracing.api.subscription.Operand.OperandCase.EXPRESSION => {
          val op = new Operand()
          op.setExpression(mapExpressionTree(operand.getExpression))
          op
        }
        case com.expedia.open.tracing.api.subscription.Operand.OperandCase.FIELD => getFieldOperand(operand)
      }
    }).toList
    expression.setOperands(operands.asJava)
    expression.setOperator(getOperator(expressionTree.getOperator))
    expression
  }


  def getOperator(operator: ExpressionTree.Operator): model.Operator = {
    operator match {
      case ExpressionTree.Operator.AND => model.Operator.AND
      case ExpressionTree.Operator.OR => model.Operator.OR
    }
  }

  def getOperator(operator: model.Operator): ExpressionTree.Operator = {
    operator match {
      case model.Operator.AND => ExpressionTree.Operator.AND
      case model.Operator.OR  => ExpressionTree.Operator.OR
    }
  }

}
