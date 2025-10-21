# streamlit run DB-live.py 
import os
import time
import streamlit as st
import duckdb
from duckdb import CatalogException
# ===== 경로 설정 =====
DB_PATH  = "database.duckdb"
LOANS_CSV_PATH = "도서대출기록.csv"
STORED_BOOKS_CSV_PATH = "소장도서목록(2025.09.11.).csv"
LOANS_TABLE = "loansBooks"
WISHLIST_TABLE = "user_wishlist"
STORED_BOOKS_TABLE = "storedBooks"

st.title("📊 도서대출기록 DuckDB 실시간(동시) 갱신 & 뷰어")

# ===== 유틸 함수 =====
def get_csv_mtime(path: str) -> float | None:
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return None

def load_csv_to_duckdb(con: duckdb.DuckDBPyConnection, csv_path: str, table: str):
    # ignore_errors=true 추가하여 문제 있는 행 무시
    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_csv_auto(?, HEADER=TRUE, AUTO_DETECT=TRUE, IGNORE_ERRORS=TRUE);
    """, [csv_path])

def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?;", [table]
    ).fetchone()[0] > 0

# ===== 상태 초기화(Session State) =====
if "loans_csv_mtime" not in st.session_state:
    st.session_state.loans_csv_mtime = get_csv_mtime(LOANS_CSV_PATH)
if "stored_books_csv_mtime" not in st.session_state:
    st.session_state.stored_books_csv_mtime = get_csv_mtime(STORED_BOOKS_CSV_PATH)
if "last_refresh_at" not in st.session_state:
    st.session_state.last_refresh_at = None

# ===== 연결 & 최초 테이블 보장 =====
con = duckdb.connect(DB_PATH)

try:
    # 대출 기록 테이블 초기화
    if not table_exists(con, LOANS_TABLE):
        if st.session_state.loans_csv_mtime is None:
            st.error(f"대출 CSV 파일이 없습니다: {LOANS_CSV_PATH}")
        else:
            load_csv_to_duckdb(con, LOANS_CSV_PATH, LOANS_TABLE)
            st.success(f"최초 1회: '{LOANS_TABLE}' 테이블을 CSV로 생성했습니다.")
    
    # 소장도서 테이블 초기화
    if not table_exists(con, STORED_BOOKS_TABLE):
        if st.session_state.stored_books_csv_mtime is None:
            st.error(f"소장도서 CSV 파일이 없습니다: {STORED_BOOKS_CSV_PATH}")
        else:
            load_csv_to_duckdb(con, STORED_BOOKS_CSV_PATH, STORED_BOOKS_TABLE)
            st.success(f"최초 1회: '{STORED_BOOKS_TABLE}' 테이블을 CSV로 생성했습니다.")
    
    # 테이블 존재 여부 확인
    tables_to_check = [WISHLIST_TABLE, STORED_BOOKS_TABLE, LOANS_TABLE]
    for table in tables_to_check:
        if table_exists(con, table):
            st.sidebar.success(f"✅ {table} 테이블 존재")
        else:
            st.sidebar.warning(f"⚠️ {table} 테이블 없음")
        
except Exception as e:
    st.error(f"DB 초기화 중 오류: {e}")
    con.close()
    st.stop()

# ===== 상단 요약/컨트롤 =====
with st.expander("⚙️ 경로/상태"):
    st.write(f"**DB**: `{DB_PATH}`")
    st.write(f"**대출 CSV**: `{LOANS_CSV_PATH}`")
    st.write(f"**소장도서 CSV**: `{STORED_BOOKS_CSV_PATH}`")
    st.write(f"**대출테이블**: `{LOANS_TABLE}`")
    st.write(f"**찜하기테이블**: `{WISHLIST_TABLE}`")
    st.write(f"**소장도서테이블**: `{STORED_BOOKS_TABLE}`")
    st.write(f"대출 CSV 수정시각: {st.session_state.loans_csv_mtime}")
    st.write(f"소장도서 CSV 수정시각: {st.session_state.stored_books_csv_mtime}")

# ===== 탭으로 구분 =====
tab1, tab2, tab3 = st.tabs(["📚 도서 대출 기록", "📖 소장도서 목록", "❤️ 찜 목록"])

with tab1:
    # 기존 대출 기록 표시
    try:
        if table_exists(con, LOANS_TABLE):
            df_head = con.execute(f"SELECT * FROM {LOANS_TABLE}").fetchdf()
            st.dataframe(df_head, use_container_width=True)
            st.write(f"총 {len(df_head)}개 행 모두 표시")
        else:
            st.warning("대출 기록 테이블이 존재하지 않습니다.")
    except CatalogException as e:
        st.error(f"대출 테이블 조회 실패: {e}")

with tab2:
    # 소장도서 목록 표시
    try:
        if table_exists(con, STORED_BOOKS_TABLE):
            # 데이터 조회
            stored_books_df = con.execute(f"SELECT * FROM {STORED_BOOKS_TABLE}").fetchdf()
            
            if len(stored_books_df) > 0:
                st.subheader("📖 전체 소장도서 목록")
                
                # 검색 기능 추가
                col1, col2 = st.columns([3, 1])
                with col1:
                    search_term = st.text_input("🔍 도서명, 저자, 출판사 검색", key="book_search")
                
                # 검색 적용
                if search_term:
                    # 실제 컬럼명에 맞게 검색 쿼리 수정
                    columns = stored_books_df.columns.tolist()
                    search_conditions = []
                    
                    # 가능한 검색 컬럼들
                    possible_columns = ['도서명', '저자', '출판사', 'title', 'author', 'publisher']
                    
                    for col in possible_columns:
                        if col in columns:
                            search_conditions.append(f"{col} LIKE '%{search_term}%'")
                    
                    if search_conditions:
                        search_query = f"""
                        SELECT * FROM {STORED_BOOKS_TABLE} 
                        WHERE {' OR '.join(search_conditions)}
                        """
                        stored_books_df = con.execute(search_query).fetchdf()
                        st.write(f"검색 결과: {len(stored_books_df)}건")
                
                # 데이터프레임 표시
                st.dataframe(stored_books_df, use_container_width=True)
                
                # 통계 정보
                st.subheader("📊 소장도서 통계")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("총 소장도서 수", len(stored_books_df))
                with col2:
                    # 카테고리 수
                    category_columns = ['category', '분류', '카테고리']
                    category_col = next((col for col in category_columns if col in stored_books_df.columns), None)
                    if category_col:
                        category_count = stored_books_df[category_col].nunique()
                        st.metric("카테고리 수", category_count)
                    else:
                        st.metric("컬럼 수", len(stored_books_df.columns))
                with col3:
                    # 저자 수
                    author_columns = ['author', '저자']
                    author_col = next((col for col in author_columns if col in stored_books_df.columns), None)
                    if author_col:
                        author_count = stored_books_df[author_col].nunique()
                        st.metric("저자 수", author_count)
                    else:
                        st.metric("데이터 행수", len(stored_books_df))
                with col4:
                    # 출판사 수
                    publisher_columns = ['publisher', '출판사']
                    publisher_col = next((col for col in publisher_columns if col in stored_books_df.columns), None)
                    if publisher_col:
                        publisher_count = stored_books_df[publisher_col].nunique()
                        st.metric("출판사 수", publisher_count)
                    else:
                        st.metric("테이블", STORED_BOOKS_TABLE)
                
                # 카테고리별 통계
                if category_col:
                    st.subheader("📈 카테고리별 도서 현황")
                    category_stats = con.execute(f"""
                        SELECT {category_col}, COUNT(*) as count 
                        FROM {STORED_BOOKS_TABLE} 
                        GROUP BY {category_col} 
                        ORDER BY count DESC
                    """).fetchdf()
                    st.dataframe(category_stats, use_container_width=True)
                    
            else:
                st.info("소장도서 목록이 비어있습니다.")
        else:
            st.warning(f"{STORED_BOOKS_TABLE} 테이블이 존재하지 않습니다.")
            
    except Exception as e:
        st.error(f"소장도서 목록 조회 실패: {e}")
        st.write("에러 상세:", str(e))

with tab3:
    # 찜 목록 표시
    try:
        if table_exists(con, WISHLIST_TABLE):
            wishlist_df = con.execute(f"SELECT * FROM {WISHLIST_TABLE} ORDER BY wish_date DESC").fetchdf()
            if len(wishlist_df) > 0:
                st.dataframe(wishlist_df, use_container_width=True)
                st.write(f"총 {len(wishlist_df)}개 찜 항목")
                
                # 통계
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("전체 찜 수", len(wishlist_df))
                with col2:
                    unique_users = wishlist_df['user_id'].nunique()
                    st.metric("찜한 사용자 수", unique_users)
                with col3:
                    unique_books = wishlist_df['book_id'].nunique()
                    st.metric("찜된 도서 수", unique_books)
            else:
                st.info("찜 목록이 비어있습니다.")
        else:
            st.warning("user_wishlist 테이블이 존재하지 않습니다.")
    except Exception as e:
        st.error(f"찜 목록 조회 실패: {e}")

# ===== CSV 변경 감지 → 자동 반영 =====
try:
    # 대출 CSV 변경 감지
    current_loans_mtime = get_csv_mtime(LOANS_CSV_PATH)
    if current_loans_mtime is not None and current_loans_mtime != st.session_state.loans_csv_mtime:
        load_csv_to_duckdb(con, LOANS_CSV_PATH, LOANS_TABLE)
        st.session_state.loans_csv_mtime = current_loans_mtime
        st.sidebar.success("📚 대출 CSV 변경 감지 → 테이블 갱신됨")
    
    # 소장도서 CSV 변경 감지
    current_stored_books_mtime = get_csv_mtime(STORED_BOOKS_CSV_PATH)
    if current_stored_books_mtime is not None and current_stored_books_mtime != st.session_state.stored_books_csv_mtime:
        load_csv_to_duckdb(con, STORED_BOOKS_CSV_PATH, STORED_BOOKS_TABLE)
        st.session_state.stored_books_csv_mtime = current_stored_books_mtime
        st.sidebar.success("📖 소장도서 CSV 변경 감지 → 테이블 갱신됨")
        
except Exception as e:
    st.sidebar.warning(f"CSV 자동 반영 경고: {e}")

# ===== 새로고침 컨트롤 =====
cols = st.columns(4)
with cols[0]:
    refresh_sec = st.number_input("UI 새로고침(초)", 1, 60, 3, key="ui_refresh_sec")
with cols[1]:
    auto_ui = st.checkbox("UI 자동 새로고침", value=True, key="auto_ui")
with cols[2]:
    if st.button("🔄 지금 새로고침"):
        st.rerun()

st.caption(f"Last updated: {time.strftime('%H:%M:%S')}")

# ===== 마무리 =====
con.close()

# UI 자동 새로고침
if auto_ui:
    time.sleep(int(refresh_sec))
    st.rerun()
