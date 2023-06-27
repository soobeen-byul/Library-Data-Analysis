# 📚 Library-Data-Analysis
전국 공공 도서관 이용행태 분석

## 원천 데이터 : 문화 빅데이터 플랫폼
- 공공 도서관 대출정보 (2023.01 ~ 2023.02)
- 도서관 정보 (2023.05)
- 공공 도서관 소장도서 (2023.05)

## 데이터 파이프라인
![image](https://github.com/soobeen-byul/Library-Data-Analysis/assets/95599133/34bae4c3-e632-4820-bdd9-1be3a9f312ec)
##### ✔ spark-step-1 : 필요 column만 추출한 base table 생성
    - loan_base : 월별 데이터를 통합한 2023년 1월~5월 대여 정보 테이블 생성
    - lbrry_base : 도서관 정보 (도서관명, 도서관 코드, 주소 등)
    - book_base : 도서 정보 (일렼번호, 제목명, 저자명, ISBN 등)
##### ✔ spark-step-2 : 도서관-대여-도서 테이블 병합 후 partition table 생성
    - partition : LBRRY_CD (도서관 코드)

## 기술 스택

<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white"> <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"> <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=Apache%20Spark&logoColor=white"> <img src="https://img.shields.io/badge/Apache%20Zeppelin-D22128?style=for-the-badge&logo=Apache&logoColor=white"> 



