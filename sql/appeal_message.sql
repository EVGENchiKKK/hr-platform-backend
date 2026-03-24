CREATE TABLE IF NOT EXISTS appeal_message (
  Appeal_Message_ID INT NOT NULL AUTO_INCREMENT,
  Appeal_ID INT NOT NULL,
  Author_ID INT NOT NULL,
  AM_content TEXT NOT NULL,
  AM_created DATETIME DEFAULT CURRENT_TIMESTAMP,
  AM_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (Appeal_Message_ID),
  KEY idx_appeal_message_appeal (Appeal_ID),
  KEY idx_appeal_message_author (Author_ID),
  KEY idx_appeal_message_created (AM_created),
  CONSTRAINT fk_appeal_message_appeal FOREIGN KEY (Appeal_ID) REFERENCES appeal (Appeal_ID) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_appeal_message_author FOREIGN KEY (Author_ID) REFERENCES user (User_ID) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT appeal_message_chk_1 CHECK (CHAR_LENGTH(AM_content) >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
