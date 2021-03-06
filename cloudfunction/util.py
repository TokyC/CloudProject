import boto3

from secrets import aws_access_key_id, aws_secret_access_key


def loadJsonFromS3(file_name) :
    file_name = file_name + ".json"
    client = boto3.client('s3',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    s3_object = client.get_object(Bucket='tokyaws', Key=file_name)
    body = s3_object['Body']
    response = body.read().decode("utf-8")
    return response


def createItemVideo(item, name) :
    user_name = name
    url = item["display_url"]
    nb_like = item["edge_media_preview_like"]["count"]
    nb_view = item["video_view_count"]
    nb_comment = item["edge_media_to_comment"]["count"]
    is_video = item["is_video"]
    nb_tags = len(item["tags"])
    comments_disabled = item["comments_disabled"]
    description = item["edge_media_to_caption"]["edges"][0]["node"]["text"]
    new_video = (user_name, url, comments_disabled, description, nb_like, nb_comment, is_video, nb_tags, nb_view)
    return new_video


def createItemImage(item, name) :
    user_name = name
    url = item["display_url"]
    nb_like = item["edge_media_preview_like"]["count"]
    nb_view = item["edge_media_preview_like"]["count"]
    nb_comment = item["edge_media_to_comment"]["count"]
    is_video = item["is_video"]
    nb_tags = len(item["tags"])
    comments_disabled = item["comments_disabled"]
    description = item["edge_media_to_caption"]["edges"][0]["node"]["text"]
    new_image = (user_name, url, comments_disabled, description, nb_like, nb_comment, is_video, nb_tags, nb_view)
    return new_image


def saveJsonToDB(content,name, cnx) :
    # content = data["GraphImages"]
    add_content = (
        "INSERT INTO data_ig (user_name, url, comment_is_disabled, description, nb_like, nb_comment, is_video, nb_tags, nb_views)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    try :
        for item in content :
            if item["is_video"] == True :
                add_video = createItemVideo(item, name)

                cursor = cnx.cursor()
                print("Insert a video")
                cursor.execute(add_content, add_video)
            else :
                add_image = createItemImage(item, name)
                cursor = cnx.cursor()
                print("Insert an Image")
                cursor.execute(add_content, add_image)

    finally :
        cnx.commit()
        cursor.close()
        cnx.close()


    return "Done"
