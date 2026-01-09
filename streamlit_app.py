# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders! :cup_with_straw:")
st.write(
  """Orders That Needs To Be Filled.
  """
)

session = get_active_session()
#my_dataframe = session.table("smoothies.public.Orders").filter(col('Order_Filled')==0).collect()
my_dataframe = session.table("smoothies.public.Orders")
editable_df = st.data_editor(my_dataframe,width="content")
#st.dataframe(data=my_dataframe, use_container_width=True)
submitted = st.button('Submit')
if submitted:
    try:
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        og_dataset.merge(edited_dataset
                     , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                     , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                    )
    except:
        st.write('Somthing Went Wrong!!!!')
    
