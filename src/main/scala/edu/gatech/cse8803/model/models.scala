/**
 * @author Hengte Lin
 */

package edu.gatech.cse8803.model

import java.util.Date

case class Diagnostic(patientID:String, code: String)

case class PatientInfo(patientID:String,gender:String,age:Int)

case class LabResult(patientID: String, date: Date, testName: String, value: Double)

case class Medication(patientID: String, medicine: String)

case class TextData(patientID:String, textdata: String)
