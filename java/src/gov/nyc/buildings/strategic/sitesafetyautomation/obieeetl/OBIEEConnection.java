package gov.nyc.buildings.strategic.sitesafetyautomation.obieeetl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nyc.buildings.strategic.sitesafetyautomation.connection.ConnectionFactory;
import gov.nyc.buildings.strategic.sitesafetyautomation.util.Util;

public class OBIEEConnection {
	private static final Logger logger = LogManager.getLogger(OBIEEConnection.class);

	public static void main(String[] args) {

		File activePermits = new File("X:\\BAT\\SITE SAFETY AUTOMATION\\active permitsAuto.csv");
		File activePermitsDobNow = new File("X:\\BAT\\SITE SAFETY AUTOMATION\\Active Permits - DOB NOWAuto.csv");
		File constuctionRelatedAccidentsMatchedToBisActivePermits = new File(
				"X:\\BAT\\SITE SAFETY AUTOMATION\\construction related accidents_3 YRS PERIODAuto.csv");
		File cOfOIssued = new File(
				"X:\\BAT\\SITE SAFETY AUTOMATION\\C of O Issued (BIS)Auto.csv");

		Connection con1 = ConnectionFactory.getConnection("obiee");
		Connection con2 = ConnectionFactory.getConnection("obiee");
		Connection con3 = ConnectionFactory.getConnection("obiee");
		Connection con4 = ConnectionFactory.getConnection("obiee");

		try {
			
			// BIS active permit

			if (!activePermits.exists()) {
				activePermits.createNewFile();
			} else {
				java.util.Date date = new java.util.Date();
				SimpleDateFormat sdft = new SimpleDateFormat("yyyyMdHHmmssSSS");

				String timestamp = sdft.format(date).toString();
				String path = "X:\\BAT\\SITE SAFETY AUTOMATION\\archive\\active Permits" + timestamp + ".csv";
				System.out.println("starting backing up old file");
				activePermits.renameTo(new File(path));
				System.out.println("done");
				activePermits = new File("X:\\BAT\\SITE SAFETY AUTOMATION\\active permitsAuto.csv");
			}

			BufferedWriter activePermitsWriter = new BufferedWriter(new FileWriter(activePermits));
			activePermitsWriter.write(
					"Borough Name,Job Type,Permit Type,Job-Permit,Job Number,BIN Number,issue_YEAR,issue_DATE,Permit Filing Status (Raw),Permit Sequence Number Max Flag,Permit Filing Status,Permit Sub Type,Borough Name,expire_DATE,Current Job Status,Existing Stories,Proposed Stories,Latitude Point,Longitude Point,Applicant License Type,Applicant License Number,Applicant Last Name,Applicant First Name,Applicant Middle Initial,Applicant Business Name,Community Board,Address\n");

			String activePermitSql = "SELECT \"- Permit Key Fields\".\"Borough Name\" saw_0, \"- Permit Key Fields\".\"Job Type\" saw_1, \"- Permit Key Fields\".\"Permit Type\" saw_2, case when concat(\"- Permit Key Fields\".\"Job Type\",\"- Permit Key Fields\".\"Permit Type\")= 'NBNB' then '1. NB' when concat(\"- Permit Key Fields\".\"Job Type\",\"- Permit Key Fields\".\"Permit Type\")= 'A1AL' then '2. A1' when \"- Permit Key Fields\".\"Job Type\"='A2' then '3. A2' when \"- Permit Key Fields\".\"Job Type\"='A3' then '4. A3'  when concat(\"- Permit Key Fields\".\"Job Type\",\"- Permit Key Fields\".\"Permit Type\")= 'DMDM'  then '5. DM' when \"- Permit Key Fields\".\"Job Type\"='SG' then '6. SG'  else 'Other' end saw_3, \"- Permit Key Fields\".\"Job Number\" saw_4, \"- Permit Key Fields\".\"BIN Number\" saw_5, \"- Permit Issuance Date\".D_YEAR saw_6, \"- Permit Issuance Date\".D_DATE saw_7, \"- Permit Key Fields\".\"Permit Filing Status (Raw)\" saw_8, \"- Permit Key Fields\".\"Permit Sequence Number Max Flag\" saw_9, \"- Permit Key Fields\".\"Permit Filing Status\" saw_10, \"- Permit Key Fields\".\"Permit Sub Type\" saw_11, \"- Permit Key Fields\".\"Borough Name\" saw_12, \"- Permit Expiration Date\".D_DATE saw_13, \"- Key Job Information (Job Number and Status)\".\"Current Job Status\" saw_14, \"- 13 - Building Characteristics\".\"Existing Stories\" saw_15, \"- 13 - Building Characteristics\".\"Proposed Stories\" saw_16, \"- Key Segment\".\"Latitude Point\" saw_17, \"- Key Segment\".\"Longitude Point\" saw_18, \"- Applicant Segment\".\"Applicant License Type\" saw_19, \"- Applicant Segment\".\"Applicant License Number\" saw_20, \"- Applicant Segment\".\"Applicant Last Name\" saw_21, \"- Applicant Segment\".\"Applicant First Name\" saw_22, \"- Applicant Segment\".\"Applicant Middle Initial\" saw_23, \"- Applicant Segment\".\"Applicant Business Name\" saw_24, \"- 1- Location Information\".\"Community Board\" saw_25, \"- 1- Location Information\".\"Job Location House Number\" saw_26, \"- 1- Location Information\".\"Job Location Street Name\" saw_27 FROM \"DOB - Job Filings, v 3.0\" WHERE (\"- Permit Key Fields\".\"Borough Name\" IN ('Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island')) AND ((\"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '10%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '11%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '20%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '21%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '30%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '31%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '40%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '41%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '50%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '51%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '12%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '22%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '32%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '42%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '52%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '14%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '24%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '34%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '44%' OR \"- Key Job Information (Job Number and Status)\".\"Job Number\" LIKE '54%')) AND (\"- Applicant Segment\".\"Applicant Email\" NOT LIKE '%buildings.nyc.gov%') AND (case when concat(\"- Permit Key Fields\".\"Job Type\",\"- Permit Key Fields\".\"Permit Type\")= 'NBNB' then '1. NB' when concat(\"- Permit Key Fields\".\"Job Type\",\"- Permit Key Fields\".\"Permit Type\")= 'A1AL' then '2. A1' when \"- Permit Key Fields\".\"Job Type\"='A2' then '3. A2' when \"- Permit Key Fields\".\"Job Type\"='A3' then '4. A3'  when concat(\"- Permit Key Fields\".\"Job Type\",\"- Permit Key Fields\".\"Permit Type\")= 'DMDM'  then '5. DM' when \"- Permit Key Fields\".\"Job Type\"='SG' then '6. SG'  else 'Other' end IN ('1. NB', '2. A1', '5. DM', '3. A2', '4. A3')) AND (\"- Permit Key Fields\".\"BIN Number\" NOT IN ('1813361', '1813248', '1813359', '1813360')) AND (\"- Permit Expiration Date\".D_DATE >=  VALUEOF(\"currentdate\")) AND (\"- Key Job Information (Job Number and Status)\".\"Current Job Status\" IN ('Q', 'R')) AND (\"- Permit Key Fields\".\"Permit Sequence Number Max Flag\" = 'Y') ORDER BY saw_5, saw_4, saw_7 DESC, saw_3";
			Statement activePermitStmt = con1.createStatement();
			ResultSet rsActivePermit = activePermitStmt.executeQuery(activePermitSql);
			// int i = 1;
			System.out.println("reading active permit....");
			while (rsActivePermit.next()) {

				String t1 = rsActivePermit.getString("saw_0");
				t1 = (t1 == null ? "" : t1);
				String t2 = rsActivePermit.getString("saw_1");
				t2 = (t2 == null ? "" : t2);
				String t3 = rsActivePermit.getString("saw_2");
				t3 = (t3 == null ? "" : t3);
				String t4 = rsActivePermit.getString("saw_3");
				t4 = (t4 == null ? "" : t4);
				String t5 = rsActivePermit.getString("saw_4");
				t5 = (t5 == null ? "" : t5);
				String t6 = rsActivePermit.getString("saw_5");
				t6 = (t6 == null ? "" : t6);
				String t7 = rsActivePermit.getString("saw_6");
				t7 = (t7 == null ? "" : t7);
				String t8 = rsActivePermit.getString("saw_7");
				t8 = (t8 == null ? "" : t8);
				String t9 = rsActivePermit.getString("saw_8");
				t9 = (t9 == null ? "" : t9);
				String t10 = rsActivePermit.getString("saw_9");
				t10 = (t10 == null ? "" : t10);
				String t11 = rsActivePermit.getString("saw_10");
				t11 = (t11 == null ? "" : t11);

				String t12 = rsActivePermit.getString("saw_11");
				t12 = (t12 == null ? "" : t12);
				String t13 = rsActivePermit.getString("saw_12");
				t13 = (t13 == null ? "" : t13);
				String t14 = rsActivePermit.getString("saw_13");
				t14 = (t14 == null ? "" : t14);
				String t15 = rsActivePermit.getString("saw_14");
				t15 = (t15 == null ? "" : t15);
				String t16 = rsActivePermit.getString("saw_15");
				t16 = (t16 == null ? "" : t16);

				String t17 = rsActivePermit.getString("saw_16");
				t17 = (t17 == null ? "" : t17);

				Double t18 = rsActivePermit.getDouble("saw_17");
				String t18s = (t18 == null ? "" : t18.toString());

				Double t19 = rsActivePermit.getDouble("saw_18");
				String t19s = (t19 == null ? "" : t19.toString());
				String t20 = rsActivePermit.getString("saw_19");
				t20 = (t20 == null ? "" : t20);

				String t21 = rsActivePermit.getString("saw_20");
				t21 = (t21 == null ? "" : t21);
				if (t21.contains(",")) {
					t21 = "\"" + t21 + "\"";
				}
				;

				String t22 = rsActivePermit.getString("saw_21");
				t22 = (t22 == null ? "" : t22);
				if (t22.contains(",")) {
					t22 = "\"" + t22 + "\"";
				}
				;

				String t23 = rsActivePermit.getString("saw_22");
				t23 = (t23 == null ? "" : t23);
				if (t23.contains(",")) {
					t23 = "\"" + t23 + "\"";
				}
				;
				String t24 = rsActivePermit.getString("saw_23");
				t24 = (t24 == null ? "" : t24);
				if (t24.contains(",")) {
					t24 = "\"" + t24 + "\"";
				}
				;
				String t25 = rsActivePermit.getString("saw_24");
				t25 = (t25 == null ? "" : t25);
				if (t25.contains(",")) {
					t25 = "\"" + t25 + "\"";
				}
				;
				String t26 = rsActivePermit.getString("saw_25");
				t26 = (t26 == null ? "" : t26);
				if (t26.contains(",")) {
					t26 = "\"" + t26 + "\"";
				}
				;
				String t27 = rsActivePermit.getString("saw_26");
				t27 = (t27 == null ? "" : t27);
				if (t27.contains(",")) {
					t27 = "\"" + t27 + "\"";
				}
				;
				t27 = t27.trim();

				String t28 = rsActivePermit.getString("saw_27");
				t28 = (t28 == null ? "" : t28);
				if (t28.contains(",")) {
					t28 = "\"" + t28 + "\"";
				}
				;
				t28 = t28.replaceAll(",", ".");
				;
				t28 = t28.replaceAll("[^A-Za-z0-9_]", " ");
				t28 = t28.trim();

				String line = t1 + "," + t2 + "," + t3 + "," + t4 + "," + t5 + "," + t6 + "," + t7 + "," + t8 + "," + t9
						+ "," + t10 + "," + t11 + "," + t12 + "," + t13 + "," + t14 + "," + t15 + "," + t16 + "," + t17
						+ "," + t18s + "," + t19s + "," + t20 + "," + t21 + "," + t22 + "," + t23 + "," + t24 + ","
						+ t25 + "," + t26 + "," + t27 + " " + t28 + "\n";

				activePermitsWriter.write(line);

			}
			System.out.println("done");

			Util.close(activePermitsWriter);

			// DOB NOW
			if (!activePermitsDobNow.exists()) {
				activePermitsDobNow.createNewFile();
			} else {
				java.util.Date date = new java.util.Date();
				SimpleDateFormat sdft = new SimpleDateFormat("yyyyMdHHmmssSSS");

				String timestamp = sdft.format(date).toString();
				String path1 = "X:\\BAT\\SITE SAFETY AUTOMATION\\archive\\Active Permits - DOB NOW" + timestamp + ".csv";
				System.out.println("starting backing up old file");
				System.out.println(path1);
				System.out.println(activePermitsDobNow.getAbsolutePath());
				activePermitsDobNow.renameTo(new File(path1));
				System.out.println("done");
				activePermitsDobNow = new File("X:\\BAT\\SITE SAFETY AUTOMATION\\Active Permits - DOB NOWAuto.csv");
			}

			BufferedWriter activePermitsDobNowWriter = new BufferedWriter(new FileWriter(activePermitsDobNow));
			activePermitsDobNowWriter.write(
					"Job Number,Filing Type,JobType,Filing Status,Bin,ExisitingBuildingStories,Work Type Name,Permit Issued Date,Permit Expiration Date,Sequence Number,Latitude,Longitude,Borough,License,License Number,Last Name,First Name,MiddleI nitial,Business Name,Community Board,Address\n");

			String activePermitsDobNowSql = "SELECT \"Job Filing\".\"Job Number\" saw_0, \"Job Filing\".FilingStatusType saw_1, \"Job Filing\".JobType saw_2, \"Job Filing\".CurrentFilingStatus saw_3, \"Job Filing\".Bin saw_4, \"Job Filing\".ExisitingBuildingStories saw_5, \"Work Type\".\"Work Type Name\" saw_6, \"Work Permit\".PermitIssuedDate saw_7, \"Work Permit\".PermitExpirationDate saw_8, \"Work Permit\".Workpermit_SequenceNumber saw_9, \"Property Profile\".Latitude saw_10, \"Property Profile\".Longitude saw_11, \"Work Permit\".Borough saw_12, \"Work Permit\".License saw_13, \"Work Permit\".\"License Number\" saw_14, \"Work Permit\".\"Last Name\" saw_15, \"Work Permit\".\"First Name\" saw_16, \"Work Permit\".\"MiddleI nitial\" saw_17, \"Work Permit\".\"Business Name\" saw_18, \"Job Filing\".\"Community Board\" saw_19, \"Work Permit\".\"House No\" saw_20, \"Work Permit\".\"Street Name\" saw_21 FROM \"DOB NOW - Build\" WHERE (\"Job Filing\".CurrentFilingStatus IN ('Permit Issued', 'Permit Entire')) AND (\"Work Permit\".PermitExpirationDate >=  VALUEOF(\"CurrentDate\")) AND (\"Work Type\".\"Work Type Name\" IN ('Scaffold', 'Side Walk Shed', 'Fence')) ORDER BY saw_0, saw_1, saw_2, saw_3, saw_4, saw_5, saw_6, saw_7, saw_8, saw_9, saw_10, saw_11, saw_12, saw_13, saw_14, saw_15, saw_16, saw_17, saw_18, saw_19, saw_20, saw_21";
			Statement activePermitsDobNowStmt = con2.createStatement();
			ResultSet rsActivePermitsDobNow = activePermitsDobNowStmt.executeQuery(activePermitsDobNowSql);
			// int i = 1;
			System.out.println("reading active permit dob now....");
			while (rsActivePermitsDobNow.next()) {

				String t1 = rsActivePermitsDobNow.getString("saw_0");
				t1 = (t1 == null ? "" : t1);
				String t2 = rsActivePermitsDobNow.getString("saw_1");
				t2 = (t2 == null ? "" : t2);
				String t3 = rsActivePermitsDobNow.getString("saw_2");
				t3 = (t3 == null ? "" : t3);
				String t4 = rsActivePermitsDobNow.getString("saw_3");
				t4 = (t4 == null ? "" : t4);
				String t5 = rsActivePermitsDobNow.getString("saw_4");
				t5 = (t5 == null ? "" : t5);
				String t6 = rsActivePermitsDobNow.getString("saw_5");
				t6 = (t6 == null ? "" : t6);
				String t7 = rsActivePermitsDobNow.getString("saw_6");
				t7 = (t7 == null ? "" : t7);
				Date t8 = rsActivePermitsDobNow.getDate("saw_7");
				SimpleDateFormat sdft8 = new SimpleDateFormat("M/d/yyyy");
				String t8s = (t8 == null ? "" : sdft8.format(t8));

				Date t9 = rsActivePermitsDobNow.getDate("saw_8");
				SimpleDateFormat sdft9 = new SimpleDateFormat("M/d/yyyy");
				String t9s = (t9 == null ? "" : sdft9.format(t9));

				String t10 = rsActivePermitsDobNow.getString("saw_9");
				t10 = (t10 == null ? "" : t10);

				Double t11 = rsActivePermitsDobNow.getDouble("saw_10");
				String t11s = (t11 == null ? "" : t11.toString());

				Double t12 = rsActivePermitsDobNow.getDouble("saw_11");
				String t12s = (t12 == null ? "" : t12.toString());

				String t13 = rsActivePermitsDobNow.getString("saw_12");
				t13 = (t13 == null ? "" : t13);

				String t14 = rsActivePermitsDobNow.getString("saw_13");
				t14 = (t14 == null ? "" : t14);
				t14 = t14.substring(0, 2).trim();

				String t15 = rsActivePermitsDobNow.getString("saw_14");
				t15 = (t15 == null ? "" : t15);
				String t16 = rsActivePermitsDobNow.getString("saw_15");
				t16 = (t16 == null ? "" : t16);
				t16 = t16.replaceAll(",", ".");
				;
				t16 = t16.replaceAll("[^A-Za-z0-9_]", " ");
				t16 = t16.trim();

				String t17 = rsActivePermitsDobNow.getString("saw_16");
				t17 = (t17 == null ? "" : t17);
				t17 = t17.replaceAll(",", ".");
				;
				t17 = t17.replaceAll("[^A-Za-z0-9_]", " ");
				t17 = t17.trim();

				String t18 = rsActivePermitsDobNow.getString("saw_17");
				t18 = (t18 == null ? "" : t18);
				t18 = t18.replaceAll(",", ".");
				;
				t18 = t18.replaceAll("[^A-Za-z0-9_]", " ");
				t18 = t18.trim();

				String t19 = rsActivePermitsDobNow.getString("saw_18");
				t19 = (t19 == null ? "" : t19);
				if (t19.contains(",")) {
					t19 = "\"" + t19 + "\"";
				}
				;
				t19 = t19.replaceAll(",", ".");
				;
				t19 = t19.replaceAll("[^A-Za-z0-9_]", " ");
				t19 = t19.trim();

				String t20 = rsActivePermitsDobNow.getString("saw_19");
				t20 = (t20 == null ? "" : t20);

				String t21 = rsActivePermitsDobNow.getString("saw_20");
				t21 = (t21 == null ? "" : t21);
				if (t21.contains(",")) {
					t21 = "\"" + t21 + "\"";
				}
				;

				String t22 = rsActivePermitsDobNow.getString("saw_21");
				t22 = (t22 == null ? "" : t22);
				if (t22.contains(",")) {
					t22 = "\"" + t22 + "\"";
				}
				;
				t22 = t22.replaceAll(",", ".");
				;
				t22 = t22.replaceAll("[^A-Za-z0-9_]", " ");
				t22 = t22.trim();

				String line = t1 + "," + t2 + "," + t3 + "," + t4 + "," + t5 + "," + t6 + "," + t7 + "," + t8s + ","
						+ t9s + "," + t10 + "," + t11s + "," + t12s + "," + t13 + "," + t14 + "," + t15 + "," + t16
						+ "," + t17 + "," + t18 + "," + t19 + "," + t20 + "," + t21 + " " + t22 + "\n";

				activePermitsDobNowWriter.write(line);

			}
			System.out.println("done");

			Util.close(activePermitsDobNowWriter);

			// accident report

			if (!constuctionRelatedAccidentsMatchedToBisActivePermits.exists()) {
				constuctionRelatedAccidentsMatchedToBisActivePermits.createNewFile();
			} else {
				java.util.Date date = new java.util.Date();
				SimpleDateFormat sdft = new SimpleDateFormat("yyyyMdHHmmssSSS");

				String timestamp = sdft.format(date).toString();
				String path = "X:\\BAT\\SITE SAFETY AUTOMATION\\archive\\construction related accidents_3 YRS PERIOD"
						+ timestamp + ".csv";
				System.out.println("starting backing up old file");
				constuctionRelatedAccidentsMatchedToBisActivePermits.renameTo(new File(path));
				System.out.println("done");
				constuctionRelatedAccidentsMatchedToBisActivePermits = new File(
						"X:\\BAT\\SITE SAFETY AUTOMATION\\construction related accidents_3 YRS PERIODAuto.csv");
			}

			BufferedWriter constuctionRelatedAccidentsMatchedToBisActivePermitsWriter = new BufferedWriter(
					new FileWriter(constuctionRelatedAccidentsMatchedToBisActivePermits));
			constuctionRelatedAccidentsMatchedToBisActivePermitsWriter.write(
					"Report ID,Year,Incident date (MM/DD/YYYY),Time (HH:MM_AM or PM),Check2,Incident Type,CR,Fatality,Injury,Accident,Record Type,Hazard to Responders,Boro,Address Number,Street,Block,Lot,BIN,Additional Address,Occupancy Class,Occupied Building,Building Type,Owner Name,Owner Address,Number of Open Complaint,Number of Open ECB Violations,Number of Open DOB Violations,Affiliation,Initial Detail of Accident,DOB Unit Notified,DOB Point Person,DOB Action,Detail Description,Building Collapsed,Disposition,FalseCall(True/False),Landmark(True/False),LocalLaw(True/False),Incident Level,EOC Final Description,UnionSite,BEST Tracking Number,Weather Related(True/False),Potential LoftLaw(True/False),DOB Violation Numbers,ECB Violation Numbers,SWO,HPDExecuted(True/False),Permit,Permit No,Last Open Job,Number of workers on site,Operating at time of Construction Incident,Owner's Name,ContractorLicenseClass,ContractorLicenseType,ContractorLicenseNumber,Fatality Details,Collapse Details,Equipment Details,Damage Element,Water Issue,Type of Excavation,QC Description\n");
			java.util.Date date = new java.util.Date();
			SimpleDateFormat sdftYearStamp = new SimpleDateFormat("yyyy");
			SimpleDateFormat sdftMonthDayStamp = new SimpleDateFormat("-MM-dd");
			String yearStamp =  sdftYearStamp.format(date).toString();
			String monthDayTimeStamp = sdftMonthDayStamp.format(date).toString();
			if (monthDayTimeStamp.equals("-02-29")){
				monthDayTimeStamp = "-02-28";
			}
			
			int year = Integer.parseInt(yearStamp.substring(0, 4));
			int threeYear = year-3;
			String threeYearTimeStamp = "'"+threeYear+monthDayTimeStamp+" 00:00:00' ";
			String yearTimeStamp = "'"+year+monthDayTimeStamp+" 00:00:00' ";

			String constuctionRelatedAccidentsMatchedToBisActivePermitsSql = "SELECT INCIDENT_TABLE.\"Accident Report ID\" saw_0, YEAR(INCIDENT_TABLE.\"Incident date (MM/DD/YYYY)\") saw_1, INCIDENT_TABLE.\"Incident date (MM/DD/YYYY)\" saw_2, INCIDENT_TABLE.\"Time (HH:MM_AM or PM)\" saw_3, INCIDENT_TABLE.Check2 saw_4, case WHEN INCIDENT_TABLE.Check2 = 'WF' then 'Worker Fell' WHEN INCIDENT_TABLE.Check2 = 'SC' then 'Scaffold/Shoring Installations' WHEN INCIDENT_TABLE.Check2 = 'CE' then 'Mechanical Construction Equipment' WHEN INCIDENT_TABLE.Check2 = 'E' then 'Excavation/Soil Work' WHEN INCIDENT_TABLE.Check2 = 'MF' then 'Material Failure (Fell)' WHEN  INCIDENT_TABLE.Check2 = 'OC' AND (INCIDENT_TABLE.\"Other Category (Construction)\" = 'Partial Demo' OR INCIDENT_TABLE.\"Other Category (Construction)\" = 'Demo' OR INCIDENT_TABLE.\"Other Category (Construction)\" = 'Demolition') then 'Demolition' WHEN  INCIDENT_TABLE.Check2 = 'OC' AND (INCIDENT_TABLE.\"Other Category (Construction)\" <> 'Partial Demo' OR INCIDENT_TABLE.\"Other Category (Construction)\" <> 'Demo' OR INCIDENT_TABLE.\"Other Category (Construction)\" <> "
					+ "'Demolition' OR INCIDENT_TABLE.\"Other Category (Construction)\" Null) then 'Other Construction Related' WHEN INCIDENT_TABLE.Check2 = 'S' then 'Internal Structural Damage' WHEN INCIDENT_TABLE.Check2 = 'EE' then 'Elevator/Escalator/Amusement Ride' WHEN INCIDENT_TABLE.Check2 = 'F' then 'Facade/Envelope Failure' WHEN INCIDENT_TABLE.Check2 = 'EF' then 'Explostion/Fumes/Electrocution' WHEN INCIDENT_TABLE.Check2 = 'OE' then 'Occupancy/Other' WHEN INCIDENT_TABLE.Check2 = 'RW' then 'Retaining Wall' else 'Not Yet Determined' end saw_5, case WHEN INCIDENT_TABLE.ConstructionSiteIND = 'Y' then 'Construction Related' WHEN INCIDENT_TABLE.ConstructionSiteIND = 'N' then 'Not Construction Related' else 'Not Yet Categorized' end saw_6, CASE WHEN INCIDENT_TABLE.\"Record Type\""
					+ " = 'A' THEN INCIDENT_TABLE.Fatality ELSE 0 END saw_7, CASE WHEN INCIDENT_TABLE.\"Record Type\" = 'A' THEN INCIDENT_TABLE.Injury ELSE 0 END saw_8, CASE WHEN INCIDENT_TABLE.\"Record Type\" = 'A' THEN 1 ELSE 0 END saw_9, INCIDENT_TABLE.\"Record Type\" saw_10, INCIDENT_TABLE.\"Hazard to Responders\" saw_11, INCIDENT_TABLE.Boro saw_12, INCIDENT_TABLE.\"Address Number\" saw_13, INCIDENT_TABLE.Street saw_14, INCIDENT_TABLE.Block saw_15, INCIDENT_TABLE.Lot saw_16, INCIDENT_TABLE.BIN saw_17, INCIDENT_TABLE.\"Additional Address\" saw_18, INCIDENT_TABLE.\"Occupancy Class\" saw_19, INCIDENT_TABLE.\"Occupied Building\" saw_20, INCIDENT_TABLE.\"Building Type\" saw_21, INCIDENT_TABLE.\"Owner Name\" saw_22, INCIDENT_TABLE.\"Owner Address\" saw_23, INCIDENT_TABLE.\"Number of Open Complaint\" saw_24, INCIDENT_TABLE.\"Number of Open ECB Violations\" saw_25, INCIDENT_TABLE.\"Number of Open DOB Violations\" saw_26, INCIDENT_TABLE.Affiliation saw_27, INCIDENT_TABLE.\"Initial Detail of Accident\" saw_28, INCIDENT_TABLE.\"DOB Unit Notified\" saw_29, INCIDENT_TABLE.\"DOB Point Person\" saw_30, INCIDENT_TABLE.\"DOB Action\" saw_31, INCIDENT_TABLE.\"Detail Description\" "
					+ "saw_32, INCIDENT_TABLE.\"Building Collapsed\" saw_33, INCIDENT_TABLE.Disposition saw_34, INCIDENT_TABLE.\"FalseCall(True/False)\" saw_35, INCIDENT_TABLE.\"Landmark(True/False)\" saw_36, INCIDENT_TABLE.\"LocalLaw(True/False)\" saw_37, INCIDENT_TABLE.\"Incident Level\" saw_38, INCIDENT_TABLE.\"EOC Final Description\" saw_39, INCIDENT_TABLE.UnionSite saw_40, "
					+ "INCIDENT_TABLE.\"BEST Tracking Number\" saw_41, INCIDENT_TABLE.\"Weather Related(True/False)\" saw_42, INCIDENT_TABLE.\"Potential LoftLaw(True/False)\" saw_43, INCIDENT_TABLE.\"DOB Violation Numbers\" saw_44, INCIDENT_TABLE.\"ECB Violation Numbers\" saw_45, INCIDENT_TABLE.SWO saw_46, INCIDENT_TABLE.\"HPDExecuted(True/False)\" saw_47, INCIDENT_TABLE.Permit saw_48, INCIDENT_TABLE.\"Permit No\" saw_49, INCIDENT_TABLE.\"Last Open Job\" saw_50, INCIDENT_TABLE.\"Number of workers on site\" saw_51, INCIDENT_TABLE.\"Operating at time of Construction Incident\" saw_52, INCIDENT_TABLE.\"Owner's Name\" saw_53, INCIDENT_TABLE.ContractorLicenseClass saw_54, INCIDENT_TABLE.ContractorLicenseType saw_55, INCIDENT_TABLE.ContractorLicenseNumber saw_56, INCIDENT_TABLE.\"Fatality Details\" saw_57, INCIDENT_TABLE.\"Collapse Details\" saw_58, INCIDENT_TABLE.\"Equipment Details\" saw_59, INCIDENT_TABLE.\"Damage Element\" saw_60, INCIDENT_TABLE.\"Water Issue\" saw_61, INCIDENT_TABLE.\"Type of Excavation\" saw_62, case WHEN INCIDENT_TABLE.\"QC Description\" = 'Complete' then ' Complete'"
					+ " else INCIDENT_TABLE.\"QC Description\" end saw_63 "
					+ "FROM \"DOB - INCIDENT_DATABASE\" WHERE (INCIDENT_TABLE.\"Incident date (MM/DD/YYYY)\" BETWEEN timestamp "
					+ threeYearTimeStamp
					+ "AND timestamp "
					+ yearTimeStamp
					+ ") AND (INCIDENT_TABLE.\"Record Type\" = 'A') AND (case WHEN INCIDENT_TABLE.ConstructionSiteIND = 'Y' then 'Construction Related' WHEN INCIDENT_TABLE.ConstructionSiteIND = 'N' then 'Not Construction Related' else 'Not Yet Categorized' end = 'Construction Related') ORDER BY saw_2, saw_3";
			Statement constuctionRelatedAccidentsMatchedToBisActivePermitsStmt = con3.createStatement();
			ResultSet rsConstuctionRelatedAccidentsMatchedToBisActivePermits = constuctionRelatedAccidentsMatchedToBisActivePermitsStmt
					.executeQuery(constuctionRelatedAccidentsMatchedToBisActivePermitsSql);
			// int i = 1;
			System.out.println("reading accident....");
			while (rsConstuctionRelatedAccidentsMatchedToBisActivePermits.next()) {

				String t1 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_0");
				t1 = (t1 == null ? "" : t1);
				String t2 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_1");
				t2 = (t2 == null ? "" : t2);
				Date t3 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getDate("saw_2");
				SimpleDateFormat sdft3 = new SimpleDateFormat("MM/dd/yyyy");
				String t3s = (t3 == null ? "" : sdft3.format(t3));
				Date t4 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getDate("saw_3");
				SimpleDateFormat sdft4 = new SimpleDateFormat("HH:mm:ss");
				String t4s = (t4 == null ? "" : sdft4.format(t4));
				String t5 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_4");
				t5 = (t5 == null ? "" : t5);
				String t6 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_5");
				t6 = (t6 == null ? "" : t6);
				String t7 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_6");
				t7 = (t7 == null ? "" : t7);
				String t8 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_7");
				t8 = (t8 == null ? "" : t8);
				String t9 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_8");
				t9 = (t9 == null ? "" : t9);
				String t10 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_9");
				t10 = (t10 == null ? "" : t10);
				String t11 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_10");
				t11 = (t11 == null ? "" : t11);
				
				String t12 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_11");
				t12 = (t12 == null ? "" : t12);
				String t13 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_12");
				t13 = (t13 == null ? "" : t13);
				String t14 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_13");
				t14 = (t14 == null ? "" : t14);
				String t15 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_14");
				t15 = (t15 == null ? "" : t15);
				String t16 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_15");
				t16 = (t16 == null ? "" : t16);
				

				String t17 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_16");
				t17 = (t17 == null ? "" : t17);

				String t18 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_17");
				t18 = (t18 == null ? "" : t18);

				String t19 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_18");
				t19 = (t19 == null ? "" : t19);
				String t20 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_19");
				t20 = (t20 == null ? "" : t20);

				String t21 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_20");
				t21 = (t21 == null ? "" : t21);
				if (t21.contains(",")) {
					t21 = "\"" + t21 + "\"";
				}
				;

				String t22 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_21");
				t22 = (t22 == null ? "" : t22);
				if (t22.contains(",")) {
					t22 = "\"" + t22 + "\"";
				}
				;

				String t23 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_22");
				t23 = (t23 == null ? "" : t23);
				if (t23.contains(",")) {
					t23 = "\"" + t23 + "\"";
				}
				;
				String t24 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_23");
				t24 = (t24 == null ? "" : t24);
				if (t24.contains(",")) {
					t24 = "\"" + t24 + "\"";
				}
				;
				String t25 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_24");
				t25 = (t25 == null ? "" : t25);
				if (t25.contains(",")) {
					t25 = "\"" + t25 + "\"";
				}
				;
				String t26 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_25");
				t26 = (t26 == null ? "" : t26);
				if (t26.contains(",")) {
					t26 = "\"" + t26 + "\"";
				}
				;
				String t27 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_26");
				t27 = (t27 == null ? "" : t27);
				if (t27.contains(",")) {
					t27 = "\"" + t27 + "\"";
				}
				;
				t27 = t27.trim();

				String t28 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_27");
				t28 = (t28 == null ? "" : t28);
				if (t28.contains(",")) {
					t28 = "\"" + t28 + "\"";
				}
				;
				t28 = t28.trim();
				
				
				String t29 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_28");
				t29 = (t29 == null ? "" : t29);
				t29 = t29.replaceAll(",", ".");
				;
				t29 = t29.trim();
				t29 = t29.replaceAll("[^A-Za-z0-9_]", " ");
				
				String t30 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_29");
				t30 = (t30 == null ? "" : t30);
				if (t30.contains(",")) {
					t30 = "\"" + t30 + "\"";
				}
				;
				t30 = t30.trim();
				
				String t31 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_30");
				t31 = (t31 == null ? "" : t31);
				if (t31.contains(",")) {
					t31 = "\"" + t31 + "\"";
				}
				;
				t31 = t31.trim();
				
				String t32 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_31");
				t32 = (t32 == null ? "" : t32);
				if (t32.contains(",")) {
					t32 = "\"" + t32 + "\"";
				}
				;
				t32 = t32.trim();
				
				String t33 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_32");
				t33 = (t33 == null ? "" : t33);
				t33 = t33.replaceAll(",", ".");
				;
				t33 = t33.trim();
				t33 = t33.replaceAll("[^A-Za-z0-9_]", " ");
				
				String t34 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_33");
				t34 = (t34 == null ? "" : t34);
				if (t34.contains(",")) {
					t34 = "\"" + t34 + "\"";
				}
				;
				t34 = t34.trim();
				
				String t35 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_34");
				t35 = (t35 == null ? "" : t35);
				if (t35.contains(",")) {
					t35 = "\"" + t35 + "\"";
				}
				;
				t35 = t35.trim();
				
				String t36 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_35");
				t36 = (t36 == null ? "" : t36);
				if (t36.contains(",")) {
					t36 = "\"" + t36 + "\"";
				}
				;
				t36 = t36.trim();
				
				String t37 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_36");
				t37 = (t37 == null ? "" : t37);
				if (t37.contains(",")) {
					t37 = "\"" + t37 + "\"";
				}
				;
				t37 = t37.trim();
				
				String t38 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_37");
				t38 = (t38 == null ? "" : t38);
				if (t38.contains(",")) {
					t38 = "\"" + t38 + "\"";
				}
				;
				t38 = t38.trim();
				
				String t39 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_38");
				t39 = (t39 == null ? "" : t38);
				if (t39.contains(",")) {
					t39 = "\"" + t39 + "\"";
				}
				;
				t39 = t39.trim();
				
				String t40 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_39");
				t40 = (t40 == null ? "" : t40);
				t40 = t40.replaceAll(",", ".");
				;
				t40 = t40.trim();
				t40 = t40.replaceAll("[^A-Za-z0-9_]", " ");
				
				String t41 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_40");
				t41 = (t41 == null ? "" : t41);
				if (t41.contains(",")) {
					t41 = "\"" + t41 + "\"";
				}
				;
				t41 = t41.trim();
				
				String t42 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_41");
				t42 = (t42 == null ? "" : t42);
				if (t42.contains(",")) {
					t42 = "\"" + t42 + "\"";
				}
				;
				t42 = t42.trim();
				
				String t43 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_42");
				t43 = (t43 == null ? "" : t43);
				if (t43.contains(",")) {
					t43 = "\"" + t43 + "\"";
				}
				;
				t43 = t43.trim();
				
				String t44 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_43");
				t44 = (t44 == null ? "" : t44);
				if (t44.contains(",")) {
					t44 = "\"" + t44 + "\"";
				}
				;
				t44 = t44.trim();
				
				String t45 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_44");
				t45 = (t45 == null ? "" : t45);
				if (t45.contains(",")) {
					t45 = "\"" + t45 + "\"";
				}
				;
				t45 = t45.trim();
				
				
				String t46 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_45");
				t46 = (t46 == null ? "" : t46);
				if (t46.contains(",")) {
					t46 = "\"" + t46 + "\"";
				}
				;
				t46 = t46.trim();
				
				String t47 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_46");
				t47 = (t47 == null ? "" : t47);
				if (t47.contains(",")) {
					t47 = "\"" + t47 + "\"";
				}
				;
				t47 = t47.trim();
				
				String t48 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_47");
				t48 = (t48 == null ? "" : t48);
				if (t48.contains(",")) {
					t48 = "\"" + t48 + "\"";
				}
				;
				t48 = t48.trim();
				
				
				String t49 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_48");
				t49 = (t49 == null ? "" : t49);
				if (t49.contains(",")) {
					t49 = "\"" + t49 + "\"";
				}
				;
				t49 = t49.trim();
				
				
				String t50 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_49");
				t50 = (t50 == null ? "" : t50);
				if (t50.contains(",")) {
					t50 = "\"" + t50 + "\"";
				}
				;
				t50 = t50.trim();
				
				
				String t51 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_50");
				t51 = (t51 == null ? "" : t51);
				if (t51.contains(",")) {
					t51 = "\"" + t51 + "\"";
				}
				;
				t51 = t51.trim();
				
				String t52 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_51");
				t52 = (t52 == null ? "" : t52);
				if (t52.contains(",")) {
					t52 = "\"" + t52 + "\"";
				}
				;
				t52 = t52.trim();
				
				String t53 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_52");
				t53 = (t53 == null ? "" : t53);
				if (t53.contains(",")) {
					t53 = "\"" + t53 + "\"";
				}
				;
				t53 = t53.trim();
				
				
				String t54 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_53");
				t54 = (t54 == null ? "" : t54);
				if (t54.contains(",")) {
					t54 = "\"" + t54 + "\"";
				}
				;
				t54 = t54.trim();
				
				
				String t55 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_54");
				t55 = (t55 == null ? "" : t55);
				if (t55.contains(",")) {
					t55 = "\"" + t55 + "\"";
				}
				;
				t55 = t55.trim();
				
				String t56 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_55");
				t56 = (t56 == null ? "" : t56);
				if (t56.contains(",")) {
					t56 = "\"" + t56 + "\"";
				}
				;
				t56 = t56.trim();
				
				String t57 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_56");
				t57 = (t57 == null ? "" : t57);
				if (t57.contains(",")) {
					t57 = "\"" + t57 + "\"";
				}
				;
				t57 = t57.trim();
				
				String t58 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_57");
				t58 = (t58 == null ? "" : t58);
				if (t58.contains(",")) {
					t58 = "\"" + t58 + "\"";
				}
				;
				t58 = t58.trim();
				
				String t59 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_58");
				t59 = (t59 == null ? "" : t59);
				if (t59.contains(",")) {
					t59 = "\"" + t59 + "\"";
				}
				;
				t59 = t59.trim();
				
				
				String t60 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_59");
				t60 = (t60 == null ? "" : t60);
				if (t60.contains(",")) {
					t60 = "\"" + t60 + "\"";
				}
				;
				t60 = t60.trim();
				
				String t61 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_60");
				t61 = (t61 == null ? "" : t61);
				if (t61.contains(",")) {
					t61 = "\"" + t61 + "\"";
				}
				;
				t61 = t61.trim();

				String t62 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_61");
				t62 = (t62 == null ? "" : t62);
				if (t62.contains(",")) {
					t62 = "\"" + t62 + "\"";
				}
				;
				t62 = t62.trim();
				

				String t63 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_62");
				t63 = (t63 == null ? "" : t63);
				if (t63.contains(",")) {
					t63 = "\"" + t63 + "\"";
				}
				;
				t63 = t63.trim();
				

				String t64 = rsConstuctionRelatedAccidentsMatchedToBisActivePermits.getString("saw_63");
				t64 = (t64 == null ? "" : t64);
				if (t64.contains(",")) {
					t64 = "\"" + t64 + "\"";
				}
				;
				t64 = t64.trim();
				

				String line = t1 + "," + t2 + "," + t3s + "," + t4s + "," + t5 + "," + t6 + "," + t7 + "," + t8 + "," + t9
						+ "," + t10 + "," + t11 + "," + t12 + "," + t13 + "," + t14 + "," + t15 + "," + t16 + "," + t17
						+ "," + t18 + "," + t19 + "," + t20 + "," + t21 + "," + t22 + "," + t23 + "," + t24 + ","
						+ t25 + "," + t26 + "," + t27 + ", " + t28 + "," + t29+ ","+ t30 + "," + t31 + "," + t32 + "," + t33 + "," + t34 + "," + t35 + "," + t36 
						+ "," + t37
						+ "," + t38 + "," + t39 + ","+ 
						t40 + "," + t41 + "," + t42 + "," + t43 + "," + t44 + "," + t45 + "," + t46 + "," + t47
						+ "," + t48 + "," + t49 + ","
						
						+
						t50 + "," + t51 + "," + t52 + "," + t53 + "," + t54 + "," + t55 + "," + t56 + "," + t57
						+ "," + t58 + "," + t59 + ","
						+t60 + "," + t61 + "," + t62 + "," + t63 + "," + t64 + "\n";

				// System.out.println("reading record " + t1s + " " + t2s + "
				// ");
				// System.out.print("-");
				// System.out.println(t1 + "," + t2 + "," + t3 + "," + t4 + ","
				// + t5 + "," + t6 + "," + t7 + "," + t8 + ","
				// + t9 + "," + t10 + "," + t11 + "," + t12 + "," + t13 + "," +
				// t14 + "," + t15 + "," + t16 + ","
				// + t17 + "," + t18 + "," + t19 + "," + t20 + "," + t21 + "," +
				// t22);
				// i++;

				constuctionRelatedAccidentsMatchedToBisActivePermitsWriter.write(line);

			}
			System.out.println("done");

			Util.close(constuctionRelatedAccidentsMatchedToBisActivePermitsWriter);
			
			
			// C of O
			

			if (!cOfOIssued.exists()) {
				cOfOIssued.createNewFile();
			} else {
				java.util.Date dateHere = new java.util.Date();
				SimpleDateFormat sdft = new SimpleDateFormat("yyyyMdHHmmssSSS");

				String timestamp = sdft.format(dateHere).toString();
				String path = "X:\\BAT\\SITE SAFETY AUTOMATION\\archive\\C of O Issued (BIS)" + timestamp + ".csv";
				System.out.println("starting backing up old file");
				cOfOIssued.renameTo(new File(path));
				System.out.println("done");
				cOfOIssued = new File("X:\\BAT\\SITE SAFETY AUTOMATION\\C of O Issued (BIS)Auto.csv");
			}

			BufferedWriter cOfOIssuedWriter = new BufferedWriter(new FileWriter(cOfOIssued));
			cOfOIssuedWriter.write(
					"J_BIN_NUMBER,C of O Issue Date,D_MONTH,D_YEAR,J_BORO,J_BLOCK,J_LOT,J_HOUSE_NUMBER,J_STREET_NAME,J_JOB_NUMBER,J_JOB_TYPE,Application Status (Raw),Filing Status (Raw),AP_A_PRIOR_CO_NUMBER,Record Type,AP_KEY_TEXT,AP_ITEM_NUMBER,AP_A_ISSUE_TYPE,Count Submissions\n");

			String cOfOIssuedSql = "SELECT \"- Job Table Doc 1 only\".J_BIN_NUMBER saw_0, \"- C of O Issue Date\".D_DATE saw_1, \"- C of O Issue Date\".D_MONTH saw_2, \"- C of O Issue Date\".D_YEAR saw_3, \"- Job Table Doc 1 only\".J_BORO saw_4, \"- Job Table Doc 1 only\".J_BLOCK saw_5, \"- Job Table Doc 1 only\".J_LOT saw_6, \"- Job Table Doc 1 only\".J_HOUSE_NUMBER saw_7, \"- Job Table Doc 1 only\".J_STREET_NAME saw_8, \"- Job Table Doc 1 only\".J_JOB_NUMBER saw_9, \"- Job Table Doc 1 only\".J_JOB_TYPE saw_10, \"- Application Segment\".\"Application Status (Raw)\" saw_11, \"- Application Segment\".\"Filing Status (Raw)\" saw_12, \"- Application Segment\".AP_A_PRIOR_CO_NUMBER saw_13, \"- Key Segment\".\"Record Type\" saw_14, \"- Key Segment\".AP_KEY_TEXT saw_15, \"- Key Segment\".AP_ITEM_NUMBER saw_16, \"- Key Segment\".AP_A_ISSUE_TYPE saw_17, \"- C of O Fact\".\"Count Submissions\" saw_18 FROM \"DOB - Certificate of Occupancy\" WHERE (\"- Key Segment\".\"Record Type\" = 'Application') AND (\"- Key Segment\".AP_A_ISSUE_TYPE IN ('F', 'T')) AND (\"- C of O Issue Date\".D_DATE >= date '2016-01-01') ORDER BY saw_9";
			Statement cOfOIssuedStmt = con4.createStatement();
			ResultSet rsCOfOIssued = cOfOIssuedStmt.executeQuery(cOfOIssuedSql);
			// int i = 1;
			System.out.println("reading C of O....");
			while (rsCOfOIssued.next()) {
				String t1 = rsCOfOIssued.getString("saw_0");
				t1 = (t1 == null ? "" : t1);
				Date t2 = rsCOfOIssued.getDate("saw_1");
				SimpleDateFormat sdft2 = new SimpleDateFormat("M/d/yyyy");
				
				String t2s = (t2 == null ? "" : sdft2.format(t2));
				
				String t3 = rsCOfOIssued.getString("saw_2");
				t3 = (t3 == null ? "" : t3);
				String t4 = rsCOfOIssued.getString("saw_3");
				t4 = (t4 == null ? "" : t4);
				String t5 = rsCOfOIssued.getString("saw_4");
				t5 = (t5 == null ? "" : t5);
				String t6 = rsCOfOIssued.getString("saw_5");
				t6 = (t6 == null ? "" : t6);
				String t7 = rsCOfOIssued.getString("saw_6");
				t7 = (t7 == null ? "" : t7);
				if (t7.contains(",")) {
					t7 = "\"" + t7 + "\"";
				}
				;
				String t8 = rsCOfOIssued.getString("saw_7");
				t8 = (t8 == null ? "" : t8);
				String t9 = rsCOfOIssued.getString("saw_8");
				t9 = (t9 == null ? "" : t9);
				t9 = t9.replaceAll(",", ".");
				;
				t9 = t9.replaceAll("[^A-Za-z0-9_]", " ");
				t9 = t9.trim();
				String t10 = rsCOfOIssued.getString("saw_9");
				
				t10 = (t10 == null ? "" : t10);
				String t11 = rsCOfOIssued.getString("saw_10");
				t11 = (t11 == null ? "" : t11);

				String t12 = rsCOfOIssued.getString("saw_11");
				t12 = (t12 == null ? "" : t12);
				String t13 = rsCOfOIssued.getString("saw_12");
				t13 = (t13 == null ? "" : t13);
				String t14 = rsCOfOIssued.getString("saw_13");
				t14 = (t14 == null ? "" : t14);
				if (t14.contains(",")) {
					t14 = "\"" + t14 + "\"";
				}
				;
				String t15 = rsCOfOIssued.getString("saw_14");
				t15 = (t15 == null ? "" : t15);
				String t16 = rsCOfOIssued.getString("saw_15");
				t16 = (t16 == null ? "" : t16);

				String t17 = rsCOfOIssued.getString("saw_16");
				t17 = (t17 == null ? "" : t17);

				String t18 = rsCOfOIssued.getString("saw_17");
				t18 = (t18 == null ? "" : t18);

				String t19 = rsCOfOIssued.getString("saw_18");
				t19 = (t19 == null ? "" : t19);
				

				String line = t1 + "," + t2s + "," + t3 + "," + t4 + "," + t5 + "," + t6 + "," + t7 + "," + t8 + "," + t9
						+ "," + t10 + "," + t11 + "," + t12 + "," + t13 + "," + t14 + "," + t15 + "," + t16 + "," + t17
						+ "," + t18 + "," + t19 + "\n";

				cOfOIssuedWriter.write(line);

			}
			System.out.println("done");

			Util.close(cOfOIssuedWriter);


		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			Util.close(con1);
			Util.close(con2);
			Util.close(con3);
			Util.close(con4);
			// Util.close(con5);
			// Util.close(con6);
			// Util.close(con7);
			// Util.close(con8);
			// Util.close(con9);
			// Util.close(con10);
			// Util.close(con11);
			// Util.close(con12);
			// Util.close(con13);
		}

	}

}
