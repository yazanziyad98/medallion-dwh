  CREATE TABLE `wages` (
  `Src_Code` int NOT NULL,
  `Src_Name` varchar(100) NOT NULL,
  `Work_Type` varchar(100) NOT NULL,
  `National_Number` varchar(10) NOT NULL,
  `SSN` decimal(10,0) NOT NULL,
  `Personal_Number` decimal(10,0) DEFAULT NULL,
  `Card_Number` decimal(10,0) DEFAULT NULL,
  `Wage` decimal(10,3) DEFAULT NULL,
  `Last_Subscription_Date` date DEFAULT NULL,
  `Pension_Type` varchar(200) DEFAULT NULL,
  `Sctr` varchar(50) DEFAULT NULL,
  `Last_Active_Subscription_Date` date DEFAULT NULL,
  `Last_Active_subscription_Count` decimal(5,0) DEFAULT NULL,
  `First_Subscription_Date` date DEFAULT NULL,
  `Total_Subscriptions_Count` decimal(5,0) DEFAULT NULL,
  `Workplace_Name` varchar(600) DEFAULT NULL,
  `Workplace_Number` decimal(10,0) DEFAULT NULL,
  `Sctr_Code` decimal(2,0) DEFAULT NULL,
  `Sub_Sector_Code` decimal(4,0) DEFAULT NULL,
  `Brnch_Code` decimal(2,0) DEFAULT NULL,
  `Load_Date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



CREATE TABLE `Individual_info` (
  `National_Number` varchar(10) NOT NULL,
  `First_Name` varchar(30) DEFAULT NULL,
  `Second_Name` varchar(20) DEFAULT NULL,
  `Family_Name` varchar(20) DEFAULT NULL,
  `Gender` varchar(1) DEFAULT NULL,
  `Social_Status_Code` varchar(2) DEFAULT NULL,
  `Nationality_Code` varchar(3) DEFAULT NULL,
  `Country_Code` varchar(10) DEFAULT NULL,
  `Governorate_Code` varchar(2) DEFAULT NULL,
  `City_Code` varchar(10) DEFAULT NULL,
  `Birth_Date` date DEFAULT NULL,
  `Father_Nat_Number` varchar(10) DEFAULT NULL,
  `Mother_Nat_Number` varchar(10) DEFAULT NULL,
  `Civil_Reg_Office_Code` varchar(3) DEFAULT NULL,
  `Civil_Registration_Number` varchar(7) DEFAULT NULL,
  `Family_Book_Number` varchar(7) DEFAULT NULL,
  `Family_Book_Issue_Date` date DEFAULT NULL,
  `ID_Number` varchar(8) DEFAULT NULL,
  `ID_Issue_Date` date DEFAULT NULL, 
  `ID_Expired_Date` date DEFAULT NULL,
  `Passport_Pub_Office_Code` varchar(3) DEFAULT NULL,
  `Passport_Number` varchar(8) DEFAULT NULL,
  `Passport_Issue_date` date DEFAULT NULL,
  `IS_Alive` varchar(1) DEFAULT NULL,
  `Family_Status` varchar(2) DEFAULT NULL,
  `Birth_Country` varchar(10) DEFAULT NULL,
  `Load_Date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


 CREATE TABLE `insured_information` (
  `SSN` decimal(10,0) NOT NULL,
  `National_Number` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `Personal_Number` decimal(10,0) DEFAULT NULL,
  `Load_Date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




CREATE TABLE `insured_wage` (
  `SSN` decimal(10,0) NOT NULL,
  `Wage_Year` decimal(4,0) NOT NULL,
  `Wage_Amount` decimal(9,1) DEFAULT NULL,
  `Load_Date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


 CREATE TABLE `insured_transaction` (
  `SSN` decimal(10,0) NOT NULL,
  `Start_Date` date NOT NULL,
  `End_Date` date DEFAULT NULL,
  `Wage_Amount` decimal(9,1) DEFAULT NULL,
  `Load_Date` datetime DEFAULT NULL,
  `message_num` int DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
