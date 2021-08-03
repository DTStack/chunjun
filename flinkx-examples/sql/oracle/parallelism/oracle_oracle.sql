-- ----------------------------
-- Table structure for oracle_all_type_source
-- ----------------------------
-- DROP TABLE "ORACLE"."oracle_all_type_source";
-- CREATE TABLE "ORACLE"."oracle_all_type_source" (
--                                                  "id" NUMBER(38,0),
--                                                  "t_number_1" NUMBER(16,32),
--                                                  "t_number_2" NUMBER(32,16),
--                                                  "t_number_3" NUMBER(8,-4),
--                                                  "t_int" NUMBER,
--                                                  "t_integer" NUMBER,
--                                                  "t_char" CHAR(255 BYTE),
--                                                  "t_varchar" VARCHAR2(255 BYTE),
--                                                  "t_varchar2" VARCHAR2(255 BYTE),
--                                                  "t_decimal" NUMBER(16,0),
--                                                  "t_numeric" NUMBER(32,0),
--                                                  "t_date" DATE,
--                                                  "t_timestamp" TIMESTAMP(6),
--                                                  "t_float" FLOAT(126),
--                                                  "t_real" FLOAT(63),
--                                                  "t_binary_double" BINARY_DOUBLE,
--                                                  "t_binary_float" BINARY_FLOAT,
--                                                  "t_char_varying" VARCHAR2(255 BYTE),
--                                                  "t_character" CHAR(255 BYTE),
--                                                  "t_character_varying" VARCHAR2(255 BYTE),
--                                                  "t_double_precision" FLOAT(126),
--                                                  "t_long" LONG,
--                                                  "t_national_char" NCHAR(255),
--                                                  "t_national_char_varying" NVARCHAR2(255),
--                                                  "t_national_character" NCHAR(255),
--                                                  "t_national_character_varying" NVARCHAR2(255),
--                                                  "t_nchar" NCHAR(255),
--                                                  "t_nchar_varying" NVARCHAR2(255),
--                                                  "t_nvarchar2" NVARCHAR2(255),
--                                                  "t_raw" RAW(255)
-- )
--     LOGGING
-- NOCOMPRESS
-- PCTFREE 10
-- INITRANS 1
-- STORAGE (
--   INITIAL 65536
--   NEXT 1048576
--   MINEXTENTS 1
--   MAXEXTENTS 2147483645
--   BUFFER_POOL DEFAULT
-- )
-- PARALLEL 1
-- NOCACHE
-- DISABLE ROW MOVEMENT
-- ;

-- ----------------------------
-- Records of oracle_all_type_sink
-- ----------------------------
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('8948001', '0.000000000000000005041841', '4463001.472545', '2240000', '5901158', '599099', '719be6772bd4efe63d1a3043a82b6e45                                                                                                                                                                                                                               ', '864afab58c7a96e9e035d551959944b5', 'e47e9630f0e2eeabec5350e5d490d72e', '409423164', '577230658', TO_DATE('2016-06-17 16:17:46', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2018-07-12 18:13:00.957000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.9048635359674937', '0.2868164378647300', '0.2518314728562352', '0.65153033', '6d791ba9ac54bed08043cae4f4c912a2', 'efe6c2bf5e19666d68aab1024512e3cc                                                                                                                                                                                                                               ', '4da41a8dd0b9b6ecddec863cf251c9d6', '0.2106533369408146', '9c2c5927e69f696b66139370fabee1c1', 'c93cda697fc2765cc236e976ae84a66a                                                                                                                                                                                                                               ', 'a704a532b1c318983381d73325cada3a', '769de7661f1a45304151f48ec44a3bfe                                                                                                                                                                                                                               ', '673e94421262c1c621dba2eabbd7b6ca', '9ac23439b4cd8cdcbec2bced3d35d2d4                                                                                                                                                                                                                               ', '2239526124e42c3a193c4daad0d721fc', '1abc18dcd3555aed269c8c64d8bda524', HEXTORAW('6462386638303938363438653463303461306237336132653530366435366436'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('6144226', '0.000000000000000008722992', '4706865.1055484', '5470000', '3585827', '1242801', '0ba039816a63a32484a742d607e20767                                                                                                                                                                                                                               ', '66a36310934b47fda11de17c4f9dbd73', '4ba17007485d38874bfebf9f3d6bf75b', '456464832', '913332971', TO_DATE('2012-09-05 13:18:51', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2020-04-23 13:38:29.163000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.4630790640138160', '0.9974636954505500', '0.1691421170964085', '0.26857576', '008aa55d90a8eb57ce0d2a9c1c9e1842', 'cdcdfbb19032f4d012bb842c16629a35                                                                                                                                                                                                                               ', 'e18d466a04da7c4c38a6f150f2758683', '0.5201392207149376', '712156087436ac6426d333c3c41fd5a7', '6fd044b7cc0259a998d847fcc950aab9                                                                                                                                                                                                                               ', '5223ad5143e7cdec9900539f8d73c035', '34cb005de825b4dfeddf920b96600d6a                                                                                                                                                                                                                               ', 'fb1ac387427666972c07558c1e2fdabc', '2e5600712ce62fca0435108c6762626b                                                                                                                                                                                                                               ', 'b3e3572bd08ad7994efa22298ef2e747', 'e6c839f592d205854a2aaf21eb97c748', HEXTORAW('6563313635656137656663626366653439323861306266363430323631336237'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('1099107', '0.000000000000000003373772', '7385843.381087', '8750000', '6535962', '6868688', '50c1a14643f043976b254793487cebbd                                                                                                                                                                                                                               ', 'f7e0fdeb1d6303b382d643278979d8be', '83797036d01ab6dd62ed023140ff4210', '332048447', '14411271', TO_DATE('2012-06-18 21:30:42', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2020-12-24 23:14:19.152000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.8289521028308331', '0.0803323473105300', '0.1144854164778983', '0.42670760', 'edff2b6e1f0f6d36805cc42509e0c7ed', '6d7f6ea95967a8c984a1960675f71d04                                                                                                                                                                                                                               ', '493c1c7e007575cfbbded7f6085055aa', '0.6858565591544545', '2fb8435c89f747aad206b1511e21cee5', '28640b4a0f6acdef2f50d7b8b3c71ab0                                                                                                                                                                                                                               ', '17186b9f5b0179962dd6d19ed54b3689', '1bab03b742e982a3a8aeee52228e761d                                                                                                                                                                                                                               ', '749aba6e86c5c2347a18efc87f97ad5b', '98afb3ee615a8343d4049da9b27d1a76                                                                                                                                                                                                                               ', '8cbd579b0de4883b930ca0ac7d7ec373', '34d4e945b9c0411ecec40572d605b73e', HEXTORAW('3833626235326336653762613536393838373634333535343536333532306162'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('211901', '0.000000000000000001494040', '4965571.7637572', '2220000', '6690452', '7121201', 'a659e9fb9a018bb81c38efeb22ab858a                                                                                                                                                                                                                               ', '6de4b26bb8bfcb02a6e2a5c8e956e75a', '3c2fe5bf1b7d291678ab6318f9077639', '687019760', '571203319', TO_DATE('2014-10-10 00:25:28', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2019-02-20 16:35:01.890000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.8157284347140055', '0.7653532249090600', '0.7789306827472169', '0.43738624', '2b0511e3ca06e0de38aff5a5e38e8759', '537aaeaf66a1ceef3a98d5b87c5469fd                                                                                                                                                                                                                               ', '8740d9a0c273e2b59166c47cb9b859cc', '0.5947806486598828', 'e078585b8c85fa8ff5e213099a19e9cd', '4a0807ebb88b27d8645b8bdb6d5b2e9a                                                                                                                                                                                                                               ', 'c23f80bb889c7f21e3822bce15e693c6', '0d1f62a21a2828b21993a5977f519a6c                                                                                                                                                                                                                               ', '8c2fa9b10055d0f7604f5965673751b7', 'bfa6c5c04ce97a66b1106f91ab8a163c                                                                                                                                                                                                                               ', '7cd52d182a864aa9a2cc2cfd276896bc', 'ee786e22805e99fc78262a63715a4c57', HEXTORAW('3730376462663232323932663839306139343934363166653165666265376461'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('7341084', '0.000000000000000008429623', '1541155.4794038', '2320000', '7247361', '5637659', '1fc15030508c3ddfe5df55ea36f96431                                                                                                                                                                                                                               ', 'b24fb5e0639b58e530103de827a73f07', 'bcf8f6ecb2cebb0ce3e545a7bef4e6b3', '978446927', '693928198', TO_DATE('2019-05-02 01:22:23', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2020-04-08 14:08:50.801000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.5643275761878317', '0.9652028248882401', '0.5099782857441291', '0.33433011', '62e2aa4dc1a28308ae71b98ea7e1e648', 'ed5f0cbd05b8ef57e838f8929aadb99b                                                                                                                                                                                                                               ', '7a0e999879926efafe0d29c1e5c3a988', '0.8365171569670862', 'e60e548b50f9844d2abf2d6529962d5a', '2b809b4f93d80e62573c00cbcdc92fb1                                                                                                                                                                                                                               ', '273dd31b7254105c058eb84713728a8c', 'c2447aa1c66920177f5c198a03b1c92b                                                                                                                                                                                                                               ', 'c66cdbe0a35a814a0357b06ce764106b', 'c7f548b17a14c506adb9e4a392b0b348                                                                                                                                                                                                                               ', 'b7aa31cfd5eef2e57a45be196e7ae2f4', '17de1866cf70d338aa0a770b24ad91cb', HEXTORAW('3766643735373461323538386332626339643961316262353961656231666436'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('4759878', '0.000000000000000005711787', '5824910.1736676', '3830000', '9299418', '4062929', '004f373ffaf033996172258f03881d9b                                                                                                                                                                                                                               ', '5b28a45bae390d2dce397128e1df1cd3', 'e7653a59d0e7bfc3e9961d9c72efde68', '799813690', '159911090', TO_DATE('2019-03-13 23:43:37', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2014-08-22 09:37:38.055000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.9905276993253660', '0.4500202991859400', '0.6372194333228880', '0.52458394', 'f1376372c23c902b52254cccdb5e2ac3', '43b582080a4d404935be69b3d633ee0f                                                                                                                                                                                                                               ', '4b636666373ae4d9318e22393b718756', '0.7278106905558077', 'bfe8b201ff83e4a62718c0f48156c558', '66186e775e14e619a8c55fc179be9577                                                                                                                                                                                                                               ', '69b0b48487d6baf477b64c8e6dd37809', 'ba8cc6888d56afeb6daef5aa5d31b34c                                                                                                                                                                                                                               ', '47544fbc666cf28a3e43ecedffd229d3', 'a51fe4e92f96041256dc3afeea4355dc                                                                                                                                                                                                                               ', 'b4db54694a46cca9e27c72193262ce81', '88faf79464916121cb360edd14736a00', HEXTORAW('3930323235336436393762656363396438643739393162613338363063323439'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('6359418', '0.0000000000000000012282', '4619405.5163978', '5220000', '3578295', '3289276', '6785bbbf0bf1d9346bb5248782f8408b                                                                                                                                                                                                                               ', 'db9c3dea0f25cf7974e6af37575a1fe2', '1ba759104b8b1aaaa6e585c1fd7f682e', '120854789', '274860238', TO_DATE('2011-11-19 14:36:30', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2012-06-13 10:29:52.136000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.3038743933638459', '0.1828606659074300', '0.6710225402738670', '0.71704644', '79ac96e23a47ac4faf015cfa99d38180', 'd9ee244a92009f3e37399e57d7d633c5                                                                                                                                                                                                                               ', '767409c9437cfe8e05cdc96136b9a2bc', '0.3832712145654176', 'd04e45e227a1e29b24c0e2828c89d09e', 'c0631f71f99125621cada0d2e7839feb                                                                                                                                                                                                                               ', '67a7445f77957231a29655d3774a0487', '60b89082a1c7757890783e8f086f6f0e                                                                                                                                                                                                                               ', '962d76664d8debb21660476c942cc374', '2ad8a4af709e199d6de9a06c63eb55ec                                                                                                                                                                                                                               ', 'eeb4eedf5a08998b21acf3cb684ce9fc', '7239c77abf10f7ec8d8ace212ffcd480', HEXTORAW('6363356632333738393734396533663533336164383136313134393739663639'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('5933758', '0.000000000000000006633657', '3976303.7537931', '7510000', '5065103', '2627050', '016385e72a0d30628148abe2e66cf32d                                                                                                                                                                                                                               ', '2f2a4ca4494c9e829a2f9c5a93e4ac50', '0522dc57cd5822d0f7ddcc5421429781', '312572202', '613727161', TO_DATE('2013-12-11 09:59:15', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2014-07-09 12:34:14.334000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.3280621384308191', '0.6639852647793399', '0.7775661483456628', '0.54460025', '3d3b67176491d27d72b13b222fc4fa47', '182868e07cb823ef03d8e63c415505ac                                                                                                                                                                                                                               ', '421b072e0d1dcf6504afbe696a4b47fc', '0.9642666676034826', 'acf368cffe20329692d1c7ec3d787336', '4575119a498b0b1e59033d2ce7769e4b                                                                                                                                                                                                                               ', '3e76cb55d6f187a67d16f3587d4bae4a', '56d6568d606d24d70999815107e9a0b5                                                                                                                                                                                                                               ', 'ffb4785595f46d6241553b0c4d0ceb13', '782b924b7e866071f2c3667771737359                                                                                                                                                                                                                               ', '8a111c2a7e7539710cbf179747cd4237', '184cef6a6e50b2b458ccf2ae21296dd9', HEXTORAW('3435386365373238393237653931623261386534613030393230346534316239'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('1659579', '0.000000000000000007024716', '4700147.312365', '5200000', '6150921', '5485070', '439a74eb0c942a2f1eb52a95bc9a6687                                                                                                                                                                                                                               ', 'c458a84542c6aabadf99332126a8edc7', '606432fe30264067a747a8249f88009b', '573236488', '120536281', TO_DATE('2015-02-28 12:01:42', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2018-08-15 16:07:08.018000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.4017859784810873', '0.5752477518319400', '0.7503096674528686', '0.83770818', 'a13671e8b33ad31b94768e95b79793a0', '29f3ec57b426a09e7fa120349de391a0                                                                                                                                                                                                                               ', 'c443f627cdf43652554bdab8a66728fa', '0.0572604475204885', '561cf06848136892cb42f6acde078ccc', '015ef6006108ad9c7762731f3e75a2e5                                                                                                                                                                                                                               ', 'd02b27171d1c9dc1e0d3cf10bfc2b9f8', 'f18c8e68ae732f657d6fad2553996975                                                                                                                                                                                                                               ', '8f82cb17026146dbf804d3952cd55bd2', 'e0ec803d5ba469cdb2dcf086fdf68ea8                                                                                                                                                                                                                               ', 'b61263469a8bd61ed8e6ce656e27edf6', '95ec274d38975446b9695b09cbe71996', HEXTORAW('3636653565623266303535306538323866303631653133316430663262346636'));
-- INSERT INTO "ORACLE"."oracle_all_type_source" VALUES ('6154962', '0.000000000000000008178609', '2255454.5419603', '4090000', '1104750', '9257247', 'c249c0ee6797a63c488f07a4e85d1a02                                                                                                                                                                                                                               ', 'ab340bdb6a7220391f357bb6d9ac8cdc', 'fc2b8de47b21fb844673dda803b4b665', '734308741', '352395464', TO_DATE('2011-05-31 08:05:14', 'SYYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('2012-11-26 00:52:10.624000', 'SYYYY-MM-DD HH24:MI:SS:FF6'), '0.3781191742672398', '0.7265186981589900', '0.4728023306806821', '0.44213539', '3e5cafa4d173611e45b7f4ce4492abc9', '74d89a8b1b35a54a3a69ffe28c56c984                                                                                                                                                                                                                               ', 'a848df9729911e546901dd6ae062102a', '0.5907362285504476', '53529fc71e12b1ed188ecbb986f4fbf2', '0bfa0ad08a046540c52d0a544fa928b4                                                                                                                                                                                                                               ', '2a24f772601a2a315c710640deaf797d', '447b6f34cff21abe4cd893871ac96942                                                                                                                                                                                                                               ', '8b1d2a46c7f36403d2f5cf0cf655084a', '60f9790034e22767461201fe020f6470                                                                                                                                                                                                                               ', '3cf67d30c22b3171256b25694ee98a5d', '765ae59316886357d52ba65276353c3c', HEXTORAW('3532333337356365386362396630363737623939396639333565373632653532'));
-- COMMIT;
-- COMMIT;


CREATE TABLE source
(
    id                                  decimal(38,0) ,
    t_binary_double                     double ,
    t_binary_float                      float ,
    t_char                              string ,
    t_char_varying                      string ,
    t_character                         string ,
    t_character_varying                 string ,
    t_date                              timestamp ,
    t_decimal                           decimal(38,0) ,
    t_double_precision                  double ,
    t_float                             double ,
    t_int                               decimal(38,0) ,
    t_integer                           decimal(38,0) ,
    t_long                              string ,
    t_national_char                     string ,
    t_national_char_varying             string ,
    t_national_character                string ,
    t_national_character_varying        string ,
    t_nchar                             string ,
    t_nchar_varying                     string ,
    t_number_1                          decimal(38,0) ,
    t_number_2                          decimal(38,0) ,
    t_number_3                          decimal(38,0) ,
    t_numeric                           decimal(38,0) ,
    t_nvarchar2                         string,
    t_raw                               bytes ,
    t_real                              double ,
    t_timestamp                         timestamp ,
    t_varchar                           string ,
    t_varchar2                          string
) WITH (
      'connector' = 'oracle-x',
      'url' = 'jdbc:oracle:thin:@localhost:1521:orcl',
      'table-name' = 'oracle_all_type_source',
      'username' = 'oracle',
      'password' = 'oracle',
      'scan.fetch-size' = '2000',
      'scan.query-timeout' = '10',
    'scan.parallelism' = '5'
      );

CREATE TABLE sink
(
    id                                  decimal(38,0) ,
    t_binary_double                     double ,
    t_binary_float                      float ,
    t_char                              string ,
    t_char_varying                      string ,
    t_character                         string ,
    t_character_varying                 string ,
    t_date                              timestamp ,
    t_decimal                           decimal(38,0),
    t_double_precision                  double ,
    t_float                             double ,
    t_int                               decimal(38,0) ,
    t_integer                           decimal(38,0) ,
    t_long                              string ,
    t_national_char                     string ,
    t_national_char_varying             string ,
    t_national_character                string ,
    t_national_character_varying        string ,
    t_nchar                             string ,
    t_nchar_varying                     string ,
    t_number_1                          decimal(38,0) ,
    t_number_2                          decimal(38,0) ,
    t_number_3                          decimal(38,0) ,
    t_numeric                           decimal(38,0) ,
    t_nvarchar2                         string,
    t_raw                               bytes ,
    t_real                              double ,
    t_timestamp                         timestamp ,
    t_varchar                           string ,
    t_varchar2                          string
) WITH (
      'connector' = 'oracle-x',
      'url' = 'jdbc:oracle:thin:@localhost:1521:orcl',
      'table-name' = 'oracle_all_type_sink_copy1',
      'username' = 'oracle',
      'password' = 'oracle',
      'sink.buffer-flush.max-rows' = '2000',
      'sink.all-replace' = 'true'
      );

insert into sink
select *
from source;
