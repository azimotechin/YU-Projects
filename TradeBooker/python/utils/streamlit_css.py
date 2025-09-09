import streamlit as st

def minimize_padding():
    st.markdown("""
        <style>
            header.stAppHeader {
                background-color: transparent;
                z-index: 0;
            }
            section.stMain .block-container {
                padding: 0rem 0.75rem;
                z-index: 1;
            }
        </style>
                
    """, unsafe_allow_html=True)

def configure_navbar(page_title):
    minimize_padding()

    navbar_html = """
    <style>
        .navbar {
            position: fixed;
            top: 100;
            left: 0;
            right: 0;
            height: 24px;
            background: linear-gradient(90deg, #1e3a8a 0%, #ea580c 100%);
            z-index: 1000;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .navbar-title {
            color: white;
            font-weight: normal;
            font-size: 16px;
            margin: 0;
        }
        
        .hamburger-menu {
            position: relative;
            display: inline-block;
        }
        
        .hamburger-icon {
            display: flex;
            flex-direction: column;
            cursor: pointer;
            padding: 5px;
        }
        
        .hamburger-icon span {
            width: 20px;
            height: 2px;
            background-color: white;
            margin: 2px 0;
            transition: 0.3s;
        }
        
        .dropdown-content {
            display: none;
            position: absolute;
            right: 0;
            background-color: white;
            min-width: 120px;
            box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
            border-radius: 4px;
            overflow: hidden;
        }
        
        .dropdown-content a {
            color: #333;
            padding: 10px 16px;
            text-decoration: none;
            display: block;
            font-size: 14px;
            transition: background-color 0.3s;
        }
        
        .dropdown-content a:hover {
            background-color: #f1f1f1;
        }
        
        .hamburger-menu:hover .dropdown-content {
            display: block;
        }
        
        /* Adjust main content to account for fixed navbar */
        .main > div {
            padding-top: 50px;
        }
    </style>

    <div class="navbar">
        <div class="navbar-title">${TITLE}</div>
        <div class="hamburger-menu">
            <div class="hamburger-icon">
                <span></span>
                <span></span>
                <span></span>
            </div>
            <div class="dropdown-content">
                <a href="?reload=True" target="_self" onclick="window.top.location.reload()">🔄 Reload</a>
            </div>
        </div>
    </div>
    """
    html = navbar_html.replace("${TITLE}", "&nbsp; &nbsp; &nbsp;" + page_title)

    import streamlit.components.v1 as components
    #components.html(html)
    #st.markdown(html, unsafe_allow_html=True)
    st.write(html, unsafe_allow_html=True)